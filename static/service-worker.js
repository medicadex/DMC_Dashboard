const CACHE_NAME = 'dmc-v1.3'; // Incremented version
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
  self.skipWaiting(); // Force the waiting service worker to become the active service worker
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      console.log('Opened cache');
      return cache.addAll(ASSETS_TO_CACHE);
    })
  );
});

// Fetch Event - Serve from Cache, Fallback to Network
self.addEventListener('fetch', (event) => {
  // Skip cross-origin requests
  if (!event.request.url.startsWith(self.location.origin) && !event.request.url.startsWith('https://fonts') && !event.request.url.startsWith('https://cdnjs')) {
    return;
  }

  event.respondWith(
    caches.match(event.request).then((response) => {
      // Return cache hit, else fetch from network
      return response || fetch(event.request).then(fetchRes => {
        return fetchRes;
      }).catch(() => {
        console.log('Offline: Network request failed');
      });
    })
  );
});

// Activate Event - Clean up old caches
self.addEventListener('activate', (event) => {
  event.waitUntil(clients.claim()); // Become available to all pages immediately
  const cacheWhitelist = [CACHE_NAME];
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (cacheWhitelist.indexOf(cacheName) === -1) {
            console.log('Deleting old cache:', cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    })
  );
});
