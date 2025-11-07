// Simple page view tracking
(function(){
	try{
		// Ensure a session id in cookie
		function getSID(){
			const m=document.cookie.match(/(?:^|; )sid=([^;]+)/);return m?decodeURIComponent(m[1]):null;
		}
		// Don't track admin pages
		if(location.pathname && location.pathname.indexOf('/admin') === 0){
			return;
		}
		let sid=getSID();
		if(!sid){
			sid=crypto.randomUUID?crypto.randomUUID():Math.random().toString(36).slice(2);
			// Persist for ~180 days so returning visitors keep the same SID
			var maxAge = 60*60*24*180;
			document.cookie='sid='+sid+';path=/;SameSite=Lax;max-age='+maxAge;
		}
		// Send view event
		fetch('/track',{
			method:'POST',headers:{'Content-Type':'application/json'},
			body:JSON.stringify({event:'view',path:location.pathname,referrer:document.referrer,sid})
		}).catch(()=>{});
	}catch(e){}
})();

// Simple accordion toggle (progressive enhancement)
document.addEventListener('click',function(e){
	const btn=e.target.closest('.accordion-button');
	if(!btn) return;
	const item=btn.closest('.accordion-item');
	const expanded=item.getAttribute('aria-expanded')==='true';
	// close others in same accordion
	const parent=item.parentElement;
	[...parent.querySelectorAll('.accordion-item')].forEach(i=>{
		i.setAttribute('aria-expanded','false');
		i.querySelector('.accordion-button')?.setAttribute('aria-expanded','false');
	});
	item.setAttribute('aria-expanded',expanded?'false':'true');
	btn.setAttribute('aria-expanded',expanded?'false':'true');
});
