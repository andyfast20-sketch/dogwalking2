(function(){
  const galleryEl = document.getElementById('gallery');
  if(!galleryEl) return;

  const fallback = () => {
    const placeholders = Array.from({length:12}).map((_,i)=>`https://images.placeholders.dev/?width=600&height=420&text=AI%20Dog%20${i+1}&bgColor=%23121a2b&textColor=%23e2e8f0`);
    placeholders.forEach(src => addImg(src));
  };

  const addImg = (src) => {
    const img = document.createElement('img');
    img.loading = 'lazy';
    img.src = src;
    img.alt = 'AI‑generated dog photo';
    galleryEl.appendChild(img);
  };

  // Try fetching AI‑generated dog images from a public search API (Lexica)
  fetch('https://lexica.art/api/v1/search?q=photorealistic%20dog')
    .then(r => r.json())
    .then(data => {
      const imgs = (data && data.images) ? data.images.slice(0,12) : [];
      if(!imgs.length) return fallback();
      imgs.forEach(o => {
        const src = o.src || o.image || o.url || (o.images && o.images[0]);
        if(src) addImg(src);
      });
      if(!galleryEl.children.length) fallback();
    })
    .catch(fallback);
})();