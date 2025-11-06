// Simple page view tracking
(function(){
	try{
		// Ensure a session id in cookie
		function getSID(){
			const m=document.cookie.match(/(?:^|; )sid=([^;]+)/);return m?decodeURIComponent(m[1]):null;
		}
		let sid=getSID();
		if(!sid){
			sid=crypto.randomUUID?crypto.randomUUID():Math.random().toString(36).slice(2);
			document.cookie='sid='+sid+';path=/;SameSite=Lax';
		}
		// Send view event
		fetch('/track',{
			method:'POST',headers:{'Content-Type':'application/json'},
			body:JSON.stringify({event:'view',path:location.pathname,referrer:document.referrer,sid})
		}).catch(()=>{});
	}catch(e){}
})();
