// api.js — centralized API base + helpers
(function(){
  // Prefer current hostname, HTTPS, port 5000
  window.API_BASE = `https://${window.location.hostname}:5000/api`;

  window.apiGet = async function(path) {
    const res = await fetch(`${API_BASE}${path}`);
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  };

  window.apiPost = async function(path, body) {
    const res = await fetch(`${API_BASE}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body ?? {}),
    });
    if (!res.ok) throw new Error(`API error ${res.status}`);
    return res.json();
  };
})();