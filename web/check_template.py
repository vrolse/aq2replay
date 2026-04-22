import urllib.request
url = "http://127.0.0.1:5000/view3d/chaos_2026-02-06_223923_Team1_spekk_facility2_27950.mvd2"
with urllib.request.urlopen(url) as r:
    content = r.read().decode()
checks = ["insetPanel", "PLAYER MODEL", "renderInset", "depthTest: false", "color:     col", "insetCam", "renderer.clear(true"]
for c in checks:
    print(("OK" if c in content else "MISSING"), c)
