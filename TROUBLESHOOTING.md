## Browser restart briefly shows qBittorrent Web UI

### Symptom
After clicking “Restart server” in QBTL, the browser may briefly show
the qBittorrent Web UI instead of QBTL.

### Cause
QBTL is restarted by `qbtl-run.pl`, which exits and immediately relaunches
the server on the same port.

During the very small restart window, the port may be temporarily served
by qBittorrent if it is configured to use the same port.

This is a race condition, not a crash.

### Current Status
- Expected behavior in development mode
- Restart loop is working correctly
- No data loss or corruption

### Production TODO
- Run QBTL on a dedicated port (e.g. 8081)
- Keep qBittorrent Web UI on its default port
- Optionally make port configurable via env or config file
