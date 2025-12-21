(function () {
  function ensureHeader() {
    if (document.getElementById('qbtl-header')) return;

    const header = document.createElement('div');
    header.id = 'qbtl-header';
    header.style.cssText = 'display:flex; gap:12px; align-items:center; padding:8px 0;';

    const killBtn = document.createElement('button');
    killBtn.id = 'qbtl-kill';
    killBtn.textContent = 'Kill server (dev)';
    killBtn.style.display = 'none';

    const msg = document.createElement('span');
    msg.id = 'qbtl-killmsg';

    header.appendChild(killBtn);
    header.appendChild(msg);

    document.body.prepend(header);

    fetch('/dev_info')
      .then(r => r.json())
      .then(j => { if (j && j.dev_mode) killBtn.style.display = ''; })
      .catch(() => {});

    killBtn.addEventListener('click', () => {
      fetch('/shutdown', { method: 'POST' })
        .then(r => r.json())
        .then(j => { msg.textContent = j.msg || 'Stopping…'; })
        .catch(e => { msg.textContent = String(e); });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', ensureHeader);
  } else {
    ensureHeader();
  }
})();
