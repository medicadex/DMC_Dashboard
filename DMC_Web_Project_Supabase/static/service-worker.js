const CACHE_NAME = 'dmc-v1.4'; // Increment version to force update
const ASSETS_TO_CACHE = [
  '/',
  '/dashboard',
  '/static/manifest.json',
  '/static/ie_logo.png',
  '/static/dashboard.css',
  '/static/style.css',
  '/static/dashboard.js',
  'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap',
  'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css'
];

// Install Event - Caching Assets
self.addEventListener('install', (event) => {
  console.log('SW: Installing v1.4');
  self.skipWaiting(); 
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      console.log('SW: Opened cache');
      return cache.addAll(ASSETS_TO_CACHE);
    })
  );
});

// Fetch Event - Serve from Cache, Fallback to Network
self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;

  // Skip cross-origin requests unless they are fonts/icons
  if (!event.request.url.startsWith(self.location.origin) && 
      !event.request.url.startsWith('https://fonts') && 
      !event.request.url.startsWith('https://cdnjs')) {
    return;
  }

  event.respondWith(
    caches.match(event.request).then((response) => {
      // Return cache hit, else fetch from network
      if (response) {
        return response;
      }

      return fetch(event.request).then(fetchRes => {
        return fetchRes;
      }).catch(err => {
        console.error('SW: Fetch failed:', err);
        // If it's a navigation request and it failed, we might want to return a cached offline page if we had one
      });
    })
  );
});

// Activate Event - Clean up old caches
self.addEventListener('activate', (event) => {
  console.log('SW: Activating and clearing old caches');
  event.waitUntil(clients.claim()); 
  
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (cacheName !== CACHE_NAME) {
            console.log('SW: Deleting old cache:', cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
});
