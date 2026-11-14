# /// script
# [tool.orcaslicer.plugin]
# name = "K1C Hub"
# description = "Control your Creality K1C directly from OrcaSlicer: live telemetry, WebRTC camera, file manager, print history, motion control and calibration over native WebSocket with Moonraker fallback."
# author = "kczk"
# version = "1.1.0"
# ///

import orca
import json
import os
import re
import time
import urllib.request
import urllib.parse

try:
    import socket
except Exception:
    socket = None
try:
    import struct
except Exception:
    struct = None
try:
    import base64
except Exception:
    base64 = None
try:
    import hashlib
except Exception:
    hashlib = None
try:
    import threading
except Exception:
    threading = None
try:
    import select
except Exception:
    select = None

LOG_NAME = "k1c_debug.log"
CFG_NAME = "k1c_config.json"
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

WS_GRACE = 12.0
CMD_TTL = 12.0
HTTP_AFTER = 8.0
UNDO_WINDOW = 6.0
LOG_LINE_MAX = 400

CFG = {
    "ip": "192.168.31.82", "ws_port": 9999, "ws_path": "/websocket",
    "http_fallback": True, "silent_pct": 25, "sound": True,
    "cam_url": "", "cam_kind": "mjpeg", "cam_mode": "webrtc", "cam_fps": 8,
    "rtc_port": 8000, "rtc_path": "/call/webrtc_local", "rtc_h264_first": True,
    "cam_zoom": 100, "cam_rot": 0, "cam_fliph": False, "cam_flipv": False,
    "cam_bright": 100, "cam_contrast": 100, "cam_sat": 100,
    "cam_grid": False, "cam_hud": True,
    "pid_nozzle": 220, "pid_bed": 60, "last_heat": 200,
    "ui_lang": "auto",
    "media_base": "", "media_prefix": "", "media_strip": 0,
    "media_img_prefix": "",
}

LINK = {
    "thread": None, "connected": False, "stop": False, "force_reconnect": False,
    "status": {}, "http_status": {}, "last_status": 0, "outbox": [],
    "log": [], "log_base": 0, "err": "", "down_since": 0, "reconnects": 0,
    "files": [], "files_version": 0, "files_total": 0, "file_meta": {},
    "history": [], "history_version": 0, "history_stats": {},
    "videos": [], "pending": [],
    "motion": {}, "want_motion": False, "motion_thread": None,
    "caps": {}, "caps_busy": False,
}

THUMB = {"cache": {}, "fail": {}, "queue": [], "thread": None}
RUN = {"thread": None, "queue": [], "results": [], "busy": False,
       "current": "", "long_busy": False}

CAM = {
    "thread": None, "gen": 0, "stop": True, "want": False,
    "state": "idle", "err": "", "jpeg": None, "seq": 0, "frames": 0,
    "last_store": 0.0, "stamps": [], "fps": 0.0, "kbps": 0.0, "started": 0.0,
    "sources": [], "probing": False, "snap_busy": False, "snap_result": None,
}

RTC = {"busy": False, "seq": 0, "answer": None, "err": "", "thread": None}
MEDIA = {"busy": False, "hits": [], "last": ""}

MAT_RE = re.compile(r"_(Hyper PLA|PLA|PETG|ASA\+|ASA|TPU|ABS|PA-CF|PA|PC|PET)_", re.I)


class _DummyLock(object):
    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        return False


LOCK = threading.RLock() if threading is not None else _DummyLock()


def plugin_dir():
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except Exception:
        return os.getcwd()


def log(msg, full=False):
    line = "[" + time.strftime("%H:%M:%S") + "] " + str(msg)
    ui = line if len(line) <= LOG_LINE_MAX else line[:LOG_LINE_MAX] + " ...[cut]"
    with LOCK:
        LINK["log"].append(ui)
        if len(LINK["log"]) > 1200:
            drop = len(LINK["log"]) - 800
            del LINK["log"][:drop]
            LINK["log_base"] += drop
    out = line if full else ui
    try:
        path = os.path.join(plugin_dir(), LOG_NAME)
        is_new = not os.path.exists(path)
        f = open(path, "a", encoding=("utf-8-sig" if is_new else "utf-8"))
        f.write(out + "\n")
        f.close()
    except Exception:
        pass


def load_cfg():
    try:
        f = open(os.path.join(plugin_dir(), CFG_NAME), "r", encoding="utf-8")
        data = json.loads(f.read())
        f.close()
        for key in CFG:
            if key in data:
                CFG[key] = data[key]
    except Exception:
        pass
    # Prepare the shared-state holder early so non-WS threads (motion, run,
    # thumb) can register themselves here and survive a module reimport.
    try:
        import sys
        if getattr(sys, "_k1c_shared_v2", None) is None:
            setattr(sys, "_k1c_shared_v2", {
                "link": LINK, "cfg": CFG, "lock": LOCK, "gen": 0,
            })
    except Exception:
        pass

def save_cfg():
    try:
        f = open(os.path.join(plugin_dir(), CFG_NAME), "w", encoding="utf-8")
        f.write(json.dumps(CFG, indent=2))
        f.close()
    except Exception as e:
        log("config save failed: " + str(e))


load_cfg()


def to_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


def to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def clamp(value, low, high):
    if value < low:
        return low
    if value > high:
        return high
    return value


# ---------------------------------------------------------------- HTTP layer

def http_json(path, timeout):
    url = "http://{ip}:7125{path}".format(ip=CFG["ip"], path=path)
    req = urllib.request.Request(url, method="GET")
    resp = urllib.request.urlopen(req, timeout=timeout)
    raw = resp.read().decode("utf-8", "replace")
    resp.close()
    return json.loads(raw)


def http_bytes(url, timeout):
    req = urllib.request.Request(url, method="GET")
    resp = urllib.request.urlopen(req, timeout=timeout)
    data = resp.read()
    resp.close()
    return data


def http_gcode_raw(script, timeout):
    url = "http://{ip}:7125/printer/gcode/script".format(ip=CFG["ip"])
    req = urllib.request.Request(
        url, data=json.dumps({"script": script}).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    resp = urllib.request.urlopen(req, timeout=timeout)
    data = json.loads(resp.read().decode("utf-8"))
    resp.close()
    if data.get("result") == "ok":
        return True, "ok"
    return False, str(data.get("error", {}).get("message", "unknown"))[:90]


def run_worker():
    while True:
        job = None
        with LOCK:
            if RUN["queue"]:
                job = RUN["queue"].pop(0)
                RUN["busy"] = True
                RUN["current"] = job["label"]
                RUN["long_busy"] = bool(job.get("long"))
        if job is None:
            time.sleep(0.12)
            continue
        log("GCODE -> " + job["label"])
        ok, detail = False, "unknown"
        try:
            ok, detail = http_gcode_raw(job["script"], job["timeout"])
        except Exception as e:
            detail = str(e)[:90]
            log("gcode error: " + detail)
        with LOCK:
            RUN["results"].append({"ok": ok, "label": job["label"],
                                   "detail": detail, "ts": time.time()})
            if len(RUN["results"]) > 40:
                del RUN["results"][:-40]
            RUN["busy"] = False
            RUN["long_busy"] = False
            RUN["current"] = ""


def ensure_run_thread():
    if threading is None:
        return False
    import sys
    shared = getattr(sys, "_k1c_shared_v2", None)
    if shared is not None:
        th = shared.get("run_thread")
        if th is not None and getattr(th, "is_alive", lambda: False)():
            with LOCK:
                RUN["thread"] = th
            return True
    with LOCK:
        th = RUN["thread"]
        if th is not None and th.is_alive():
            return True
        th = threading.Thread(target=run_worker)
        th.daemon = True
        RUN["thread"] = th
        if shared is not None:
            shared["run_thread"] = th
        th.start()
        return True

def gcode_async(script, label, timeout=60, long=False):
    if not ensure_run_thread():
        try:
            return http_gcode_raw(script, min(timeout, 8))
        except Exception as e:
            return False, str(e)[:80]
    with LOCK:
        RUN["queue"].append({"script": script, "label": label,
                             "timeout": timeout, "long": bool(long)})
    return True, label


def runner_public():
    with LOCK:
        return {"busy": RUN["busy"], "current": RUN["current"],
                "queued": len(RUN["queue"]), "results": list(RUN["results"][-8:])}


def machine_busy():
    with LOCK:
        st = to_int(LINK["status"].get("state", 0))
        m = dict(LINK["motion"])
        long_run = RUN["long_busy"]
        for job in RUN["queue"]:
            if job.get("long"):
                long_run = True
                break
    if st in (1, 4):
        return True, "printing"
    if str(m.get("print_state", "")) in ("printing", "paused"):
        return True, "printing"
    if m.get("sd_active"):
        return True, "printing"
    if long_run:
        return True, "calibrating"
    return False, ""


def http_status():
    try:
        path = ("/printer/objects/query?extruder=target,temperature"
                "&heater_bed=target,temperature"
                "&print_stats=state,print_duration&display_status=progress")
        res = http_json(path, 3).get("result", {}).get("status", {})
        ex = res.get("extruder", {})
        bed = res.get("heater_bed", {})
        st = res.get("print_stats", {})
        ds = res.get("display_status", {})
        state_map = {"printing": 1, "paused": 4, "complete": 5, "standby": 0, "error": 9}
        return {"ok": True,
                "nozzle_temp": to_int(ex.get("temperature", 0)),
                "nozzle_target": to_int(ex.get("target", 0)),
                "bed_temp": to_int(bed.get("temperature", 0)),
                "bed_target": to_int(bed.get("target", 0)),
                "progress": to_int(ds.get("progress", 0) * 100),
                "state_raw": state_map.get(st.get("state", "standby"), 0),
                "job_time": to_int(st.get("print_duration", 0))}
    except Exception as e:
        return {"ok": False, "err": str(e)[:90]}


# -------------------------------------------------------------- motion state

MOTION_QUERY = ("/printer/objects/query?"
                "toolhead=homed_axes,position,axis_minimum,axis_maximum,max_velocity,max_accel"
                "&gcode_move=speed_factor,extrude_factor,homing_origin,absolute_extrude"
                "&extruder=can_extrude,temperature,target"
                "&idle_timeout=state&print_stats=state&virtual_sdcard=is_active")


def motion_worker():
    while True:
        with LOCK:
            want = LINK["want_motion"]
        if not want:
            time.sleep(0.5)
            continue
        try:
            res = http_json(MOTION_QUERY, 4)
            st = res.get("result", {}).get("status", {})
            th = st.get("toolhead", {}) or {}
            gm = st.get("gcode_move", {}) or {}
            ex = st.get("extruder", {}) or {}
            it = st.get("idle_timeout", {}) or {}
            ps = st.get("print_stats", {}) or {}
            sd = st.get("virtual_sdcard", {}) or {}
            pos = th.get("position") or [0, 0, 0, 0]
            amin = th.get("axis_minimum") or [0, 0, 0, 0]
            amax = th.get("axis_maximum") or [220, 220, 220, 0]
            org = gm.get("homing_origin") or [0, 0, 0, 0]
            data = {
                "ok": True,
                "homed": str(th.get("homed_axes", "") or ""),
                "x": round(to_float(pos[0]), 2), "y": round(to_float(pos[1]), 2),
                "z": round(to_float(pos[2]), 3), "e": round(to_float(pos[3]), 2),
                "xmin": to_float(amin[0]), "ymin": to_float(amin[1]), "zmin": to_float(amin[2]),
                "xmax": to_float(amax[0], 220), "ymax": to_float(amax[1], 220),
                "zmax": to_float(amax[2], 220),
                "max_vel": to_int(th.get("max_velocity", 0)),
                "max_accel": to_int(th.get("max_accel", 0)),
                "speed_factor": round(to_float(gm.get("speed_factor", 1)) * 100),
                "extrude_factor": round(to_float(gm.get("extrude_factor", 1)) * 100),
                "z_offset": round(to_float(org[2]), 3),
                "abs_e": bool(gm.get("absolute_extrude", True)),
                "can_extrude": bool(ex.get("can_extrude", False)),
                "nozzle": to_int(ex.get("temperature", 0)),
                "nozzle_target": to_int(ex.get("target", 0)),
                "idle": str(it.get("state", "")),
                "print_state": str(ps.get("state", "")),
                "sd_active": bool(sd.get("is_active", False)),
            }
        except Exception as e:
            data = {"ok": False, "err": str(e)[:80]}
        with LOCK:
            LINK["motion"] = data
        time.sleep(1.5)


def ensure_motion_thread():
    if threading is None:
        return
    import sys
    shared = getattr(sys, "_k1c_shared_v2", None)
    # Reuse motion thread from a previous module reload if it is still alive.
    # The shared dict survives module reimports, local LINK does not.
    if shared is not None:
        th = shared.get("motion_thread")
        if th is not None and getattr(th, "is_alive", lambda: False)():
            with LOCK:
                LINK["motion_thread"] = th
            return
    with LOCK:
        th = LINK["motion_thread"]
        if th is not None and th.is_alive():
            return
        th = threading.Thread(target=motion_worker)
        th.daemon = True
        LINK["motion_thread"] = th
        if shared is not None:
            shared["motion_thread"] = th
        th.start()


def caps_worker():
    try:
        res = http_json("/printer/objects/list", 6)
        objs = res.get("result", {}).get("objects", []) or []
        macros = []
        for o in objs:
            if isinstance(o, str) and o.startswith("gcode_macro "):
                macros.append(o[len("gcode_macro "):])
        macros.sort()
        flags = {}
        for key in ("bed_mesh", "resonance_tester", "input_shaper", "probe", "z_tilt",
                    "screws_tilt_adjust", "force_move", "extruder", "heater_bed",
                    "configfile", "idle_timeout", "virtual_sdcard", "print_stats"):
            flags[key] = key in objs
        with LOCK:
            LINK["caps"] = {"ok": True, "objects": len(objs), "macros": macros,
                            "flags": flags, "ts": time.time()}
        log("caps: {o} objects, {m} macros".format(o=len(objs), m=len(macros)))
    except Exception as e:
        with LOCK:
            LINK["caps"] = {"ok": False, "err": str(e)[:80], "macros": [], "flags": {},
                            "ts": time.time()}
    with LOCK:
        LINK["caps_busy"] = False


def ensure_caps(force):
    if threading is None:
        return
    with LOCK:
        if LINK["caps_busy"]:
            return
        caps = LINK["caps"]
        age = time.time() - caps.get("ts", 0) if caps else 99999
        if not force and caps.get("ok") and age < 600:
            return
        LINK["caps_busy"] = True
        th = threading.Thread(target=caps_worker)
        th.daemon = True
    th.start()


def motion_public():
    with LOCK:
        return dict(LINK["motion"])


def caps_public():
    with LOCK:
        c = dict(LINK["caps"])
        c["busy"] = LINK["caps_busy"]
    return c


# ------------------------------------------------------------- motion actions

def jog_move(axis, dist, speed):
    busy, reason = machine_busy()
    if busy:
        return False, reason
    with LOCK:
        m = dict(LINK["motion"])
    if not m.get("ok"):
        return False, "motion state unknown"
    a = str(axis).lower()
    if a not in ("x", "y", "z"):
        return False, "bad axis"
    if a not in m.get("homed", ""):
        return False, a.upper() + " not homed"
    dist = to_float(dist)
    speed = clamp(to_float(speed, 50), 1, 300)
    cur = to_float(m.get(a, 0))
    lo = to_float(m.get(a + "min", 0))
    hi = to_float(m.get(a + "max", 220), 220)
    target = cur + dist
    if target < lo - 0.005 or target > hi + 0.005:
        return False, "out of range ({lo}..{hi})".format(lo=lo, hi=hi)
    restore = "M82" if m.get("abs_e", True) else "M83"
    script = "G91\nG1 {A}{D:.3f} F{F}\nG90\n{R}".format(
        A=a.upper(), D=dist, F=int(speed * 60), R=restore)
    return gcode_async(script, "jog {A}{D:g}".format(A=a.upper(), D=dist), 90)


def home_axes(axes):
    busy, reason = machine_busy()
    if busy:
        return False, reason
    clean = []
    for a in str(axes).upper().split():
        if a in ("X", "Y", "Z") and a not in clean:
            clean.append(a)
    script = "G28" + ((" " + " ".join(clean)) if clean else "")
    return gcode_async(script, "home " + (" ".join(clean) if clean else "all"), 180, True)


def park_move(what):
    busy, reason = machine_busy()
    if busy:
        return False, reason
    with LOCK:
        m = dict(LINK["motion"])
    if not m.get("ok"):
        return False, "motion state unknown"
    homed = m.get("homed", "")
    if what == "z":
        if "z" not in homed:
            return False, "Z not homed"
        target = max(0.0, to_float(m.get("zmax", 220), 220) - 3.0)
        return gcode_async("G90\nG1 Z{z:.2f} F900".format(z=target), "park bed", 120)
    if what == "center":
        if "x" not in homed or "y" not in homed:
            return False, "XY not homed"
        cx = to_float(m.get("xmax", 220), 220) / 2.0
        cy = to_float(m.get("ymax", 220), 220) / 2.0
        return gcode_async("G90\nG1 X{x:.1f} Y{y:.1f} F6000".format(x=cx, y=cy),
                           "center toolhead", 120)
    return False, "bad park target"


def z_offset(delta, reset):
    if reset:
        return gcode_async("SET_GCODE_OFFSET Z=0 MOVE=1", "reset Z offset", 60)
    d = clamp(to_float(delta), -1.0, 1.0)
    if abs(d) < 0.0005:
        return False, "zero delta"
    return gcode_async("SET_GCODE_OFFSET Z_ADJUST={d:.3f} MOVE=1".format(d=d),
                       "Z offset {d:+.3f}".format(d=d), 60)


def motors_off():
    busy, reason = machine_busy()
    if busy:
        return False, reason
    return gcode_async("M84", "motors off", 30)


def extrude_move(length, speed):
    busy, reason = machine_busy()
    if busy:
        return False, reason
    with LOCK:
        m = dict(LINK["motion"])
    if not m.get("ok"):
        return False, "motion state unknown"
    if not m.get("can_extrude"):
        return False, "nozzle below extrude temp"
    length = clamp(to_float(length), -200, 200)
    speed = clamp(to_float(speed, 5), 0.5, 15)
    if abs(length) < 0.01:
        return False, "zero length"
    restore = "M82" if m.get("abs_e", True) else "M83"
    script = "M83\nG1 E{L:.3f} F{F}\n{R}".format(L=length, F=int(speed * 60), R=restore)
    label = ("extrude " if length > 0 else "retract ") + "{n:g}mm".format(n=abs(length))
    return gcode_async(script, label, 180)


def filament_macro(kind):
    with LOCK:
        macros = list(LINK["caps"].get("macros", []))
    want = ("LOAD_FILAMENT", "LOAD", "M701") if kind == "load" else ("UNLOAD_FILAMENT", "UNLOAD", "M702")
    for name in macros:
        if name.upper() in want:
            return gcode_async(name, name, 300, True)
    return extrude_move(100 if kind == "load" else -100, 5)


def flow_factor(pct):
    pct = clamp(to_int(pct, 100), 50, 150)
    return gcode_async("M221 S{p}".format(p=pct), "flow {p}%".format(p=pct), 30)


def calib_run(kind, target):
    busy, reason = machine_busy()
    if busy:
        return False, reason
    with LOCK:
        flags = dict(LINK["caps"].get("flags", {}))
    if kind == "bed_mesh":
        if not flags.get("bed_mesh"):
            return False, "bed_mesh not available"
        return gcode_async("BED_MESH_CALIBRATE", "bed mesh calibrate", 900, True)
    if kind == "mesh_clear":
        if not flags.get("bed_mesh"):
            return False, "bed_mesh not available"
        return gcode_async("BED_MESH_CLEAR", "clear bed mesh", 60)
    if kind == "shaper":
        if not flags.get("resonance_tester"):
            return False, "resonance_tester not available"
        return gcode_async("SHAPER_CALIBRATE", "input shaper calibrate", 900, True)
    if kind == "pid_nozzle":
        t = clamp(to_int(target, 220), 150, 320)
        CFG["pid_nozzle"] = t
        save_cfg()
        return gcode_async("PID_CALIBRATE HEATER=extruder TARGET={t}".format(t=t),
                           "PID nozzle {t}C".format(t=t), 900, True)
    if kind == "pid_bed":
        t = clamp(to_int(target, 60), 40, 115)
        CFG["pid_bed"] = t
        save_cfg()
        return gcode_async("PID_CALIBRATE HEATER=heater_bed TARGET={t}".format(t=t),
                           "PID bed {t}C".format(t=t), 900, True)
    if kind == "save_config":
        if not flags.get("configfile"):
            return False, "configfile not available"
        return gcode_async("SAVE_CONFIG", "save config (restarts Klipper)", 180, True)
    return False, "unknown calibration"


def macro_run(name):
    busy, reason = machine_busy()
    if busy:
        return False, reason
    name = str(name).strip()
    with LOCK:
        macros = list(LINK["caps"].get("macros", []))
    real = None
    for m in macros:
        if m.upper() == name.upper():
            real = m
            break
    if real is None:
        return False, "macro not found on printer"
    return gcode_async(real, real, 900, True)


# ------------------------------------------------------------- files parsing

def guess_material(name):
    m = MAT_RE.search(name)
    return m.group(1).upper() if m else ""


def guess_duration(name):
    m = re.search(r"(\d+)h(\d+)m", name)
    if m:
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60
    m = re.search(r"(\d+)m(\d+)s", name)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.search(r"(\d+)h(?!\d)", name)
    if m:
        return int(m.group(1)) * 3600
    m = re.search(r"_(\d+)m\.gcode", name)
    if m:
        return int(m.group(1)) * 60
    return 0


def parse_files(raw):
    items = []
    if not isinstance(raw, dict):
        return items
    info = raw.get("fileInfo")
    if isinstance(info, str):
        for chunk in info.split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            parts = chunk.split(":")
            if len(parts) < 6:
                continue
            name = ":".join(parts[1:-5])
            if not name:
                continue
            items.append({
                "name": name, "root": parts[0], "path": parts[0] + "/" + name,
                "size": to_int(parts[-5]), "layer": parts[-4], "ts": to_int(parts[-3]),
                "filament": to_float(parts[-2]), "thumb": parts[-1],
                "duration": guess_duration(name), "material": guess_material(name),
            })
    elif isinstance(info, list):
        for entry in info:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name") or entry.get("fileName") or ""
            if not name:
                continue
            items.append({
                "name": name, "root": entry.get("path", ""), "path": entry.get("path", ""),
                "size": to_int(entry.get("file_size", entry.get("size", 0))),
                "layer": str(entry.get("floorHeight", "")),
                "ts": to_int(entry.get("create_time", entry.get("ctime", 0))),
                "filament": to_float(entry.get("consumables", 0)),
                "thumb": entry.get("thumbnail", ""),
                "duration": guess_duration(name), "material": guess_material(name),
            })
    return items


def files_update(raw):
    items = parse_files(raw)
    items.sort(key=lambda x: x.get("ts", 0), reverse=True)
    with LOCK:
        LINK["files"] = items
        LINK["files_version"] = LINK.get("files_version", 0) + 1
        LINK["files_total"] = to_int(raw.get("totalNum", len(items))) if isinstance(raw, dict) else len(items)


def media_url(abs_path):
    if not abs_path:
        return ""
    base = CFG.get("media_base", "") or ("http://" + str(CFG.get("ip", "")))
    base = base.rstrip("/")
    low = str(abs_path).lower()
    is_video = low.endswith(".mp4") or low.endswith(".avi") or low.endswith(".mkv")
    if is_video:
        prefix = CFG.get("media_prefix", "") or "/downloads/video/"
    else:
        prefix = CFG.get("media_img_prefix", "") or "/downloads/image/"
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    strip = to_int(CFG.get("media_strip", 0))
    p = abs_path[strip:] if strip > 0 else abs_path
    if not p.startswith("/"):
        p = "/" + p
    return base + prefix + urllib.parse.quote(p, safe="")


def history_update(msg):
    items = []
    for e in msg.get("historyList", []) or []:
        if not isinstance(e, dict):
            continue
        full = str(e.get("filename", "") or "")
        base = full.split("/")[-1] if "/" in full else full
        items.append({
            "id": e.get("id"), "name": base, "path": full,
            "size": to_int(e.get("size", 0)), "start": to_int(e.get("starttime", 0)),
            "total": to_int(e.get("totaltime", 0)), "used": to_int(e.get("usagetime", 0)),
            "filament": to_float(e.get("usagematerial", 0)),
            "finish": to_int(e.get("printfinish", 0)), "material": guess_material(base),
        })
    with LOCK:
        LINK["history"] = items
        LINK["history_stats"] = {
            "jobs": to_int(msg.get("totalJob", len(items))),
            "time": to_int(msg.get("totalUsageTime", 0)),
            "filament": to_float(msg.get("totalUsageMaterial", 0)),
        }
        LINK["history_version"] = LINK.get("history_version", 0) + 1


def videos_update(msg):
    out = []
    for e in msg.get("elapseVideoList", []) or []:
        if not isinstance(e, dict):
            continue
        out.append({
            "gcode": str(e.get("gcodename", "") or ""), "video": str(e.get("video", "") or ""),
            "cover": str(e.get("cover", "") or ""), "start": to_int(e.get("starttime", 0)),
            "size": to_int(e.get("size", 0)), "duration": to_int(e.get("duration", 0)),
        })
    with LOCK:
        LINK["videos"] = out
        LINK["history_version"] = LINK.get("history_version", 0) + 1


def history_payload():
    with LOCK:
        items = list(LINK.get("history", []))
        videos = list(LINK.get("videos", []))
        stats = dict(LINK.get("history_stats", {}))
        version = LINK.get("history_version", 0)
    for item in items:
        best = None
        for v in videos:
            if v["gcode"] != item["name"]:
                continue
            delta = abs(v["start"] - item["start"])
            if delta > 300:
                continue
            if best is None or delta < best[0]:
                best = (delta, v)
        if best:
            v = best[1]
            item["video_url"] = media_url(v["video"])
            item["cover_url"] = media_url(v["cover"])
            item["video_path"] = v["video"]
            item["video_size"] = v["size"]
            item["video_duration"] = v["duration"]
        else:
            item["video_url"] = ""
    return {"list": items, "stats": stats, "version": version}


# ---------------------------------------------------------- thumbnail worker

def resolve_thumb(name, hint):
    try:
        meta = http_json("/server/files/metadata?filename=" + urllib.parse.quote(name, safe="/"), 6)
        res = meta.get("result", {}) or {}
        with LOCK:
            LINK["file_meta"][name] = {
                "estimated_time": to_int(res.get("estimated_time", 0)),
                "filament_total": to_int(res.get("filament_total", 0)),
                "layer_height": res.get("layer_height", 0),
                "slicer": res.get("slicer", ""),
            }
        best = None
        for item in res.get("thumbnails") or []:
            if not isinstance(item, dict):
                continue
            area = to_int(item.get("width", 0)) * to_int(item.get("height", 0))
            if best is None or area > best[0]:
                best = (area, item)
        if best is not None and best[1].get("relative_path"):
            url = "http://{ip}:7125/server/files/gcodes/{p}".format(
                ip=CFG["ip"], p=urllib.parse.quote(best[1]["relative_path"], safe="/"))
            data = http_bytes(url, 8)
            if data:
                return "data:image/png;base64," + base64.b64encode(data).decode("ascii")
    except Exception:
        pass
    if hint:
        url = media_url(hint)
        if url:
            try:
                data = http_bytes(url, 8)
                if data:
                    return "data:image/png;base64," + base64.b64encode(data).decode("ascii")
            except Exception:
                pass
    return None


def thumb_worker():
    while True:
        job = None
        with LOCK:
            if THUMB["queue"]:
                job = THUMB["queue"].pop(0)
        if job is None:
            time.sleep(0.2)
            continue
        try:
            uri = resolve_thumb(job[0], job[1])
            with LOCK:
                if uri:
                    THUMB["cache"][job[0]] = uri
                else:
                    THUMB["fail"][job[0]] = "no preview"
        except Exception as e:
            with LOCK:
                THUMB["fail"][job[0]] = str(e)[:80]


def ensure_thumb_thread():
    if threading is None or base64 is None:
        return
    import sys
    shared = getattr(sys, "_k1c_shared_v2", None)
    if shared is not None:
        th = shared.get("thumb_thread")
        if th is not None and getattr(th, "is_alive", lambda: False)():
            with LOCK:
                THUMB["thread"] = th
            return
    with LOCK:
        th = THUMB["thread"]
        if th is not None and th.is_alive():
            return
        th = threading.Thread(target=thumb_worker)
        th.daemon = True
        THUMB["thread"] = th
        if shared is not None:
            shared["thumb_thread"] = th
        th.start()


def collect_thumbs(wanted):
    ready = {}
    if not isinstance(wanted, list):
        return ready
    ensure_thumb_thread()
    with LOCK:
        hints = {}
        for f in LINK["files"]:
            hints[f["name"]] = f.get("thumb", "")
        queued = 0
        for name in wanted[:16]:
            if not isinstance(name, str):
                continue
            if name in THUMB["cache"]:
                if len(ready) < 4:
                    ready[name] = {"uri": THUMB["cache"][name],
                                   "meta": LINK["file_meta"].get(name, {})}
            elif name in THUMB["fail"]:
                ready[name] = {"uri": "", "error": THUMB["fail"][name]}
            else:
                busy = False
                for job in THUMB["queue"]:
                    if job[0] == name:
                        busy = True
                        break
                if not busy and queued < 6:
                    THUMB["queue"].append((name, hints.get(name, "")))
                    queued += 1
    return ready


# ------------------------------------------------------------- media probing

def media_variants(sample):
    out = [("", 0)]
    for cut in ("/usr/data", "/usr/data/creality", "/usr/data/creality/userdata"):
        if sample.startswith(cut):
            out.append(("", len(cut)))
    for pref in ("/downloads", "/download", "/media", "/static", "/files",
                 "/file", "/video", "/videos", "/elapse", "/userdata", "/data"):
        out.append((pref, 0))
    idx = sample.rfind("/")
    if idx >= 0:
        out.append(("", idx))
    return out


def media_probe_worker(sample):
    hits = []
    found = None
    for port in (80, 8081, 7125):
        for pref, strip in media_variants(sample):
            p = sample[strip:]
            if not p.startswith("/"):
                p = "/" + p
            url = "http://{ip}:{pt}{pf}{pa}".format(
                ip=CFG["ip"], pt=port, pf=pref, pa=urllib.parse.quote(p, safe="/"))
            try:
                resp = urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=2)
                ctype = resp.headers.get("Content-Type", "")
                head = resp.read(2048)
                resp.close()
                is_img = head[:8].startswith(b"\x89PNG") or head[:2] == b"\xff\xd8" or "image" in ctype.lower()
                if is_img:
                    hits.append(url)
                    log("MEDIA HIT: " + url)
                    if found is None:
                        found = ("http://{ip}:{pt}".format(ip=CFG["ip"], pt=port), pref, strip)
            except Exception as e:
                msg = str(e)[:40]
                if "404" not in msg:
                    log("media miss: {u} {e}".format(u=url, e=msg))
    with LOCK:
        MEDIA["hits"] = hits
        MEDIA["busy"] = False
    if found:
        CFG["media_base"], CFG["media_prefix"], CFG["media_strip"] = found
        save_cfg()
        with LOCK:
            LINK["history_version"] += 1
            THUMB["cache"] = {}
            THUMB["fail"] = {}
        log("media source set: {b} prefix={p} strip={s}".format(b=found[0], p=found[1], s=found[2]))
    else:
        log("media probe: nothing found")


def media_probe():
    if threading is None:
        return False, "no threads"
    with LOCK:
        if MEDIA["busy"]:
            return False, "busy"
        sample = ""
        for v in LINK["videos"]:
            if v.get("cover"):
                sample = v["cover"]
                break
        if not sample:
            for f in LINK["files"]:
                if f.get("thumb"):
                    sample = f["thumb"]
                    break
        if not sample:
            return False, "no sample path"
        MEDIA["busy"] = True
        MEDIA["hits"] = []
        th = threading.Thread(target=media_probe_worker, args=(sample,))
        th.daemon = True
    log("media probe start: " + sample)
    th.start()
    return True, sample


def media_test_worker(url):
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=6)
        ctype = resp.headers.get("Content-Type", "")
        ranges = resp.headers.get("Accept-Ranges", "")
        data = resp.read(4096)
        total = resp.headers.get("Content-Length", "?")
        resp.close()
        magic = " ".join("{:02x}".format(b) for b in bytearray(data[:8]))
        log("URL TEST 200 {u} ct={c} len={l} ranges={r} magic={m}".format(
            u=url, c=ctype[:40], l=total, r=ranges, m=magic))
    except Exception as e:
        log("URL TEST FAIL {u} -> {e}".format(u=url, e=str(e)[:110]))
    with LOCK:
        MEDIA["busy"] = False


def media_test(url):
    if threading is None:
        return False
    with LOCK:
        if MEDIA["busy"]:
            return False
        MEDIA["busy"] = True
        th = threading.Thread(target=media_test_worker, args=(url,))
        th.daemon = True
    th.start()
    return True


def media_public():
    with LOCK:
        return {"base": CFG.get("media_base", ""), "prefix": CFG.get("media_prefix", ""),
                "strip": to_int(CFG.get("media_strip", 0)), "busy": MEDIA["busy"],
                "hits": len(MEDIA["hits"])}


# ---------------------------------------------------------------- WebRTC relay

def log_sdp(tag, raw_json):
    """Dump the full SDP into the log file so codec negotiation can be inspected."""
    try:
        obj = json.loads(raw_json)
        sdp = obj.get("sdp", "")
        log("{tag} type={t} sdp {n} bytes".format(tag=tag, t=obj.get("type", "?"), n=len(sdp)))
        log("{tag} SDP BEGIN\n{s}\n{tag} SDP END".format(tag=tag, s=sdp), full=True)
    except Exception as e:
        log(tag + " SDP parse failed: " + str(e)[:80])
        log(tag + " RAW: " + str(raw_json)[:2000], full=True)


def rtc_worker(seq, body):
    url = "http://{ip}:{p}{path}".format(ip=CFG["ip"],
                                         p=to_int(CFG["rtc_port"], 8000),
                                         path=CFG["rtc_path"] or "/call/webrtc_local")
    log("RTC offer -> " + url)
    ans, err = None, ""
    try:
        req = urllib.request.Request(url, data=body.encode("ascii"),
                                     headers={"Content-Type": "plain/text"}, method="POST")
        resp = urllib.request.urlopen(req, timeout=10)
        raw = resp.read().decode("utf-8", "replace").strip()
        resp.close()
        if raw.startswith("{"):
            ans = raw
        else:
            compact = "".join(raw.split())
            ans = base64.b64decode(compact + "=" * (-len(compact) % 4), validate=True).decode("utf-8", "replace")
        log("RTC answer {n} bytes".format(n=len(ans)))
        log_sdp("RTC ANSWER", ans)
    except Exception as e:
        err = str(e)[:160]
        log("RTC error: " + err)
    with LOCK:
        RTC["answer"] = ans
        RTC["err"] = err
        RTC["seq"] = seq
        RTC["busy"] = False


def rtc_offer(body, seq):
    if threading is None or base64 is None:
        return False
    with LOCK:
        if RTC["busy"]:
            return False
        RTC["busy"] = True
        RTC["answer"] = None
        RTC["err"] = ""
        RTC["seq"] = 0
    try:
        compact = "".join(str(body).split())
        decoded = base64.b64decode(compact + "=" * (-len(compact) % 4)).decode("utf-8", "replace")
        log_sdp("RTC OFFER", decoded)
    except Exception as e:
        log("RTC offer decode failed: " + str(e)[:80])
    with LOCK:
        th = threading.Thread(target=rtc_worker, args=(seq, body))
        th.daemon = True
    th.start()
    return True


def rtc_public():
    with LOCK:
        return {"busy": RTC["busy"], "seq": RTC["seq"],
                "answer": RTC["answer"], "err": RTC["err"]}


# ------------------------------------------------------------------- camera

def cam_candidates():
    ip = CFG["ip"]
    return [
        ("8000 \xc2\xb7 MJPEG", "http://{ip}:8000/webcam/?action=stream".format(ip=ip), "mjpeg"),
        ("8000 \xc2\xb7 snapshot", "http://{ip}:8000/webcam/?action=snapshot".format(ip=ip), "snapshot"),
        ("8080 \xc2\xb7 MJPEG", "http://{ip}:8080/?action=stream".format(ip=ip), "mjpeg"),
        ("Moonraker \xc2\xb7 webcam", "http://{ip}:7125/webcam/?action=stream".format(ip=ip), "mjpeg"),
    ]


def cam_store(jpg, interval):
    now = time.time()
    with LOCK:
        if interval > 0 and now - CAM["last_store"] < interval:
            return
        CAM["last_store"] = now
        CAM["jpeg"] = jpg
        CAM["seq"] += 1
        CAM["frames"] += 1
        CAM["stamps"].append((now, len(jpg)))
        while len(CAM["stamps"]) > 24:
            del CAM["stamps"][0]
        win = CAM["stamps"]
        if len(win) > 1:
            span = win[-1][0] - win[0][0]
            if span > 0:
                CAM["fps"] = (len(win) - 1) / span
                total = 0
                for it in win:
                    total += it[1]
                CAM["kbps"] = (total / 1024.0) / span


def cam_loop_mjpeg(url, fps, gen):
    resp = urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=8)
    interval = 1.0 / max(1, fps)
    buf = b""
    with LOCK:
        CAM["state"] = "live"
        CAM["started"] = time.time()
        CAM["stamps"] = []
    try:
        while not CAM["stop"] and CAM["gen"] == gen:
            chunk = resp.read(32768)
            if not chunk:
                raise IOError("stream ended")
            buf += chunk
            if len(buf) > 6000000:
                buf = buf[-3000000:]
            while True:
                start = buf.find(b"\xff\xd8")
                if start < 0:
                    buf = buf[-1:]
                    break
                end = buf.find(b"\xff\xd9", start + 2)
                if end < 0:
                    buf = buf[start:]
                    break
                jpg = buf[start:end + 2]
                buf = buf[end + 2:]
                cam_store(jpg, interval)
    finally:
        try:
            resp.close()
        except Exception:
            pass


def cam_loop_snapshot(url, fps, gen):
    interval = 1.0 / max(1, fps)
    with LOCK:
        CAM["state"] = "live"
        CAM["started"] = time.time()
        CAM["stamps"] = []
    while not CAM["stop"] and CAM["gen"] == gen:
        t0 = time.time()
        data = http_bytes(url, 6)
        if data[:2] != b"\xff\xd8":
            raise IOError("not a JPEG snapshot")
        cam_store(data, 0)
        time.sleep(max(0.05, interval - (time.time() - t0)))


def cam_worker(gen, url, kind, fps):
    backoff = 1.0
    while not CAM["stop"] and CAM["gen"] == gen:
        try:
            with LOCK:
                CAM["state"] = "connecting"
                CAM["err"] = ""
            if kind == "snapshot":
                cam_loop_snapshot(url, fps, gen)
            else:
                cam_loop_mjpeg(url, fps, gen)
            backoff = 1.0
        except Exception as e:
            with LOCK:
                CAM["state"] = "error"
                CAM["err"] = str(e)[:120]
            log("camera: " + str(e)[:110])
        if CAM["stop"] or CAM["gen"] != gen:
            break
        time.sleep(backoff)
        backoff = min(backoff + 1.0, 6.0)
    with LOCK:
        CAM["state"] = "idle"


def cam_start_stream():
    if threading is None or base64 is None or not CFG.get("cam_url"):
        return False
    with LOCK:
        CAM["gen"] += 1
        gen = CAM["gen"]
        CAM["stop"] = False
        CAM["want"] = True
        CAM["frames"] = 0
        CAM["stamps"] = []
        th = threading.Thread(target=cam_worker,
                              args=(gen, CFG["cam_url"], CFG.get("cam_kind", "mjpeg"),
                                    to_int(CFG["cam_fps"], 8)))
        th.daemon = True
        CAM["thread"] = th
    th.start()
    return True


def cam_stop_stream():
    with LOCK:
        CAM["stop"] = True
        CAM["want"] = False
        CAM["gen"] += 1
        CAM["state"] = "idle"


def cam_probe_worker():
    found = []
    for label, url, kind in cam_candidates():
        try:
            resp = urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=3)
            ctype = resp.headers.get("Content-Type", "")
            if "multipart" in ctype.lower():
                ok = True
            else:
                head = resp.read(4096)
                ok = head[:2] == b"\xff\xd8" or "image" in ctype.lower()
            resp.close()
            if ok:
                found.append({"label": label, "url": url, "kind": kind, "ctype": ctype[:50]})
                log("camera found: " + url)
        except Exception as e:
            log("camera miss: {u} {e}".format(u=url, e=str(e)[:50]))
    with LOCK:
        CAM["sources"] = found
        CAM["probing"] = False
        want = CAM["want"]
    if found:
        pick = None
        for item in found:
            if item["kind"] == "mjpeg":
                pick = item
                break
        if pick is None:
            pick = found[0]
        if not CFG.get("cam_url"):
            CFG["cam_url"] = pick["url"]
            CFG["cam_kind"] = pick["kind"]
            save_cfg()
        if want:
            cam_start_stream()
    log("camera detect done: {n}".format(n=len(found)))


def cam_detect():
    if threading is None:
        return False
    with LOCK:
        if CAM["probing"]:
            return False
        CAM["probing"] = True
        CAM["sources"] = []
        th = threading.Thread(target=cam_probe_worker)
        th.daemon = True
    th.start()
    return True


def snap_dir():
    return os.path.join(plugin_dir(), "snapshots")


def cam_snap_list():
    out = []
    try:
        folder = snap_dir()
        if os.path.isdir(folder):
            for name in os.listdir(folder):
                if not name.lower().endswith(".jpg"):
                    continue
                try:
                    st = os.stat(os.path.join(folder, name))
                except Exception:
                    continue
                out.append({"name": name, "size": st.st_size, "ts": int(st.st_mtime)})
    except Exception:
        pass
    out.sort(key=lambda x: x["ts"], reverse=True)
    return out[:14]


def cam_grab_one(url):
    if not url:
        return None
    try:
        resp = urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=6)
        buf = b""
        deadline = time.time() + 6
        while time.time() < deadline:
            chunk = resp.read(16384)
            if not chunk:
                break
            buf += chunk
            start = buf.find(b"\xff\xd8")
            if start >= 0:
                end = buf.find(b"\xff\xd9", start + 2)
                if end >= 0:
                    resp.close()
                    return buf[start:end + 2]
            if len(buf) > 4000000:
                buf = buf[-2000000:]
        resp.close()
    except Exception:
        pass
    return None


def cam_snapshot_worker():
    data = None
    with LOCK:
        if CAM["jpeg"] and time.time() - CAM["last_store"] < 4:
            data = CAM["jpeg"]
    if data is None and CFG.get("cam_url"):
        try:
            d = http_bytes(CFG["cam_url"].replace("action=stream", "action=snapshot"), 6)
            if d[:2] == b"\xff\xd8":
                data = d
        except Exception:
            data = None
    if data is None:
        data = cam_grab_one(CFG.get("cam_url", ""))
    result = {"ok": False, "name": "", "ts": time.time()}
    if data:
        try:
            folder = snap_dir()
            if not os.path.isdir(folder):
                os.makedirs(folder)
            name = "K1C_" + time.strftime("%Y%m%d_%H%M%S") + ".jpg"
            f = open(os.path.join(folder, name), "wb")
            f.write(data)
            f.close()
            result = {"ok": True, "name": name, "ts": time.time()}
            log("snapshot saved: " + name)
        except Exception as e:
            log("snapshot save: " + str(e)[:70])
    with LOCK:
        CAM["snap_result"] = result
        CAM["snap_busy"] = False


def cam_snapshot():
    if threading is None:
        return False
    with LOCK:
        if CAM["snap_busy"]:
            return False
        CAM["snap_busy"] = True
        th = threading.Thread(target=cam_snapshot_worker)
        th.daemon = True
    th.start()
    return True


def cam_public():
    with LOCK:
        started = CAM["started"]
        return {
            "url": CFG.get("cam_url", ""), "kind": CFG.get("cam_kind", "mjpeg"),
            "state": CAM["state"], "err": CAM["err"],
            "fps": round(CAM["fps"], 1), "kbps": round(CAM["kbps"], 1),
            "frames": CAM["frames"], "seq": CAM["seq"],
            "sources": list(CAM["sources"]), "probing": CAM["probing"],
            "busy": CAM["snap_busy"], "snap_result": CAM["snap_result"],
            "uptime": int(time.time() - started) if started and CAM["state"] == "live" else 0,
            "snaps": cam_snap_list(),
        }


def cam_settings():
    keys = ("cam_url", "cam_kind", "cam_mode", "cam_fps", "cam_zoom", "cam_rot",
            "cam_fliph", "cam_flipv", "cam_bright", "cam_contrast", "cam_sat",
            "cam_grid", "cam_hud", "sound", "pid_nozzle", "pid_bed", "last_heat",
            "rtc_port", "rtc_path", "rtc_h264_first")
    out = {}
    for k in keys:
        out[k] = CFG.get(k)
    return out


# ------------------------------------------------------------ WebSocket core

def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise IOError("socket closed by peer")
        buf += chunk
    return buf


def read_frame(sock):
    b0, b1 = struct.unpack("!BB", recv_exact(sock, 2))
    fin = b0 & 0x80
    opcode = b0 & 0x0F
    masked = b1 & 0x80
    length = b1 & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]
    mask = recv_exact(sock, 4) if masked else None
    data = recv_exact(sock, length) if length else b""
    if mask:
        data = bytes(bytearray(data[i] ^ mask[i % 4] for i in range(len(data))))
    return fin, opcode, data


def send_frame(sock, opcode, payload):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    header = bytearray()
    header.append(0x80 | opcode)
    n = len(payload)
    if n < 126:
        header.append(0x80 | n)
    elif n < 65536:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", n))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", n))
    mask = os.urandom(4)
    header.extend(mask)
    sock.sendall(bytes(header) + bytes(bytearray(payload[i] ^ mask[i % 4] for i in range(n))))


def ws_handshake(sock, host, port, path):
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\n"
        "Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
        "Origin: http://{host}:{port}\r\nUser-Agent: K1C-Controller\r\n\r\n"
    ).format(path=path, host=host, port=port, key=key)
    sock.sendall(request.encode("ascii"))
    raw = b""
    while b"\r\n\r\n" not in raw:
        chunk = sock.recv(1)
        if not chunk:
            break
        raw += chunk
        if len(raw) > 16384:
            break
    text = raw.decode("iso-8859-1", "replace")
    line = text.split("\r\n")[0] if text else "(no response)"
    return ("101" in line), line


def ws_open():
    ip = CFG["ip"]
    port = to_int(CFG["ws_port"], 9999)
    path = CFG["ws_path"] or "/"
    sock = socket.create_connection((ip, port), timeout=4)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        pass
    sock.settimeout(4)
    ok, line = ws_handshake(sock, ip, port, path)
    if not ok:
        try:
            sock.close()
        except Exception:
            pass
        raise IOError("handshake failed: " + line)
    sock.settimeout(3)
    with LOCK:
        LINK["connected"] = True
        LINK["err"] = ""
        LINK["down_since"] = 0
    LINK["force_reconnect"] = False
    return sock


def handle_text(text):
    try:
        msg = json.loads(text)
    except Exception:
        return
    if not isinstance(msg, dict):
        return
    if "retGcodeFileInfo" in msg:
        files_update(msg["retGcodeFileInfo"])
        return
    if "historyList" in msg:
        history_update(msg)
        return
    if "elapseVideoList" in msg:
        videos_update(msg)
        return
    with LOCK:
        prev = LINK["status"].get("state")
        LINK["status"].update(msg)
        LINK["last_status"] = time.time()
    if "state" in msg and msg.get("state") != prev:
        log("state -> {s}".format(s=msg.get("state")))
    elif len(text) < 300 and set(msg.keys()) != {"maxBedTemp", "maxNozzleTemp", "webrtcSupport"}:
        log("WS <- " + text)


def ws_worker(my_gen):
    import sys
    shared = getattr(sys, "_k1c_shared_v2", None)
    backoff = 1.0
    while not LINK["stop"] and (shared is None or shared.get("gen") == my_gen):
        sock = None
        reason = "unknown"
        started = time.time()
        try:
            sock = ws_open()
            log("WS connected")
            backoff = 1.0
            started = time.time()
            last_rx = time.time()
            last_tx = time.time()
            frag = b""
            while (not LINK["stop"] and not LINK["force_reconnect"]
                   and (shared is None or shared.get("gen") == my_gen)):
                with LOCK:
                    pending = LINK["outbox"][:]
                    LINK["outbox"] = []
                now = time.time()
                for item in pending:
                    ts = item.pop("_ts", now)
                    if now - ts > CMD_TTL:
                        continue
                    payload = json.dumps(item)
                    send_frame(sock, 0x1, payload)
                    last_tx = time.time()
                    log("WS -> " + payload[:200])
                if time.time() - last_tx > 20:
                    try:
                        send_frame(sock, 0x9, b"")
                        last_tx = time.time()
                    except Exception:
                        pass
                drained = 0
                while drained < 400:
                    ready, _, _ = select.select([sock], [], [], 0 if drained else 0.2)
                    if not ready:
                        break
                    fin, opcode, data = read_frame(sock)
                    drained += 1
                    last_rx = time.time()
                    if opcode == 0x9:
                        send_frame(sock, 0xA, data)
                    elif opcode == 0x8:
                        reason = "server CLOSE"
                        break
                    elif opcode in (0x0, 0x1, 0x2):
                        frag += data
                        if fin:
                            handle_text(frag.decode("utf-8", "replace"))
                            frag = b""
                if reason == "server CLOSE":
                    break
                if time.time() - last_rx > 45:
                    reason = "idle timeout"
                    break
            if LINK["force_reconnect"]:
                reason = "forced reconnect"
        except Exception as e:
            reason = type(e).__name__ + ": " + str(e)
            with LOCK:
                LINK["err"] = reason
        finally:
            alive = time.time() - started
            with LOCK:
                LINK["connected"] = False
                if LINK.get("down_since", 0) == 0:
                    LINK["down_since"] = time.time()
                LINK["reconnects"] = LINK.get("reconnects", 0) + 1
                n = LINK["reconnects"]
            LINK["force_reconnect"] = False
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
            if alive < 5:
                log("WS closed ({r}) #{n}, alive {a:.1f}s".format(r=reason, n=n, a=alive))
        if LINK["stop"] or (shared is not None and shared.get("gen") != my_gen):
            break
        wait_until = time.time() + backoff
        while time.time() < wait_until and not LINK["stop"]:
            with LOCK:
                down_since = LINK.get("down_since", 0)
            if CFG["http_fallback"] and down_since and (time.time() - down_since) > HTTP_AFTER:
                st = http_status()
                with LOCK:
                    LINK["http_status"] = st
            time.sleep(0.5)
        backoff = min(backoff + 1.0, 10.0)


def ws_available():
    return (socket is not None and struct is not None and base64 is not None
            and hashlib is not None and threading is not None and select is not None)


def ensure_thread():
    if not ws_available():
        return False
    import sys
    shared = getattr(sys, "_k1c_shared_v2", None)
    if shared is None:
        shared = {"link": LINK, "cfg": CFG, "lock": LOCK, "gen": 0}
        setattr(sys, "_k1c_shared_v2", shared)
    else:
        globals()["LINK"] = shared["link"]
        globals()["CFG"] = shared["cfg"]
        globals()["LOCK"] = shared["lock"]
    with LOCK:
        th = LINK["thread"]
        if th is not None and th.is_alive():
            return True
        shared["gen"] = shared.get("gen", 0) + 1
        my_gen = shared["gen"]
        LINK["stop"] = False
        th = threading.Thread(target=ws_worker, args=(my_gen,))
        th.daemon = True
        LINK["thread"] = th
    log("WS worker #{g} started".format(g=my_gen))
    th.start()
    return True


def ws_queue(method, params):
    ensure_thread()
    now = time.time()
    with LOCK:
        last = LINK.get("last_status", 0)
        connected = LINK["connected"]
    if not (connected or (last > 0 and now - last < 30)):
        return False
    with LOCK:
        LINK["outbox"].append({"method": method, "params": params, "_ts": now})
    return True


# -------------------------------------------------------------- print actions

def start_print(path, calibrate):
    if not path or path[0] != "/":
        return False, "bad path"
    busy, reason = machine_busy()
    if busy:
        return False, "printer busy"
    params = {"opGcodeFile": "printprt:" + path, "enableSelfTest": 1 if calibrate else 0}
    if ws_queue("set", params):
        return True, "WebSocket"
    return False, "WebSocket offline"


def print_command(cmd):
    script = {"pause": "PAUSE", "resume": "RESUME", "cancel": "CANCEL_PRINT"}.get(cmd)
    if not script:
        return False, "unknown command"
    return gcode_async(script, cmd, 120, True)


def queue_delete(kind, ident, name):
    pid = "p{a}{b}".format(a=int(time.time() * 1000), b=len(LINK["pending"]))
    with LOCK:
        LINK["pending"].append({"id": pid, "kind": kind, "target": ident,
                                "name": name, "due": time.time() + UNDO_WINDOW})
    return pid


def flush_pending():
    now = time.time()
    fire = []
    with LOCK:
        keep = []
        for item in LINK["pending"]:
            if item["due"] <= now:
                fire.append(item)
            else:
                keep.append(item)
        LINK["pending"] = keep
    for item in fire:
        if item["kind"] == "file":
            ws_queue("set", {"opGcodeFile": "deleteprt:" + item["target"]})
        elif item["kind"] == "history":
            ws_queue("set", {"deleteHistory": str(item["target"])})
    if fire:
        ws_queue("get", {"reqGcodeFile": 1})


def apply_control(kind, value):
    if kind in ("nozzle", "bed"):
        busy, reason = machine_busy()
        if busy:
            return False, "locked: " + reason
    if kind == "nozzle":
        value = clamp(to_int(value), 0, 320)
        params = {"nozzleTempControl": value}
        script = "M104 S{v}".format(v=value)
        if value >= 150:
            CFG["last_heat"] = value
            save_cfg()
    elif kind == "bed":
        value = clamp(to_int(value), 0, 115)
        params = {"bedTempControl": {"num": 0, "val": value}}
        script = "M140 S{v}".format(v=value)
    elif kind in ("model_fan", "case_fan", "aux_fan"):
        value = clamp(to_int(value), 0, 100)
        key = {"model_fan": "modelFanPct", "case_fan": "caseFanPct",
               "aux_fan": "auxiliaryFanPct"}[kind]
        params = {key: value}
        fan = {"model_fan": "model_fan", "case_fan": "chassis_fan", "aux_fan": "side_fan"}[kind]
        script = "SET_FAN_SPEED FAN={f} SPEED={s:.2f}".format(f=fan, s=value / 100.0)
    elif kind == "light":
        value = 1 if to_int(value) else 0
        params = {"lightSw": value}
        script = None
    elif kind == "feedrate":
        value = clamp(to_int(value), 10, 300)
        params = {"setFeedratePct": value}
        script = "M220 S{v}".format(v=value)
    else:
        return False, "unknown control"
    if ws_queue("set", params):
        return True, "WebSocket" if LINK["connected"] else "WebSocket (queued)"
    if CFG["http_fallback"] and script:
        return gcode_async(script, script.split("\n")[0][:40], 60)
    return False, "WebSocket offline"


def fmt_dur(seconds):
    seconds = to_int(seconds)
    if seconds <= 0:
        return "0m"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return "{h}h {m:02d}m".format(h=h, m=m) if h else "{m}m".format(m=m)


def state_key(raw):
    return {0: "standby", 1: "printing", 4: "paused", 5: "complete", 9: "error"}.get(raw, "unknown")


def snapshot():
    with LOCK:
        s = dict(LINK["status"])
        h = dict(LINK["http_status"])
        connected = LINK["connected"]
        last = LINK.get("last_status", 0)
        err = LINK["err"]
        reconnects = LINK.get("reconnects", 0)
    now = time.time()
    busy, busy_reason = machine_busy()
    if s and (now - last) < WS_GRACE:
        raw_state = to_int(s.get("state", 0))
        active = raw_state in (1, 4) or busy
        name = str(s.get("printFileName", "") or "")
        if "/" in name:
            name = name.split("/")[-1]
        err_obj = s.get("err", {}) or {}
        return {
            "transport": "ws",
            "link_state": "connected" if connected else "reconnecting",
            "hostname": s.get("hostname", "K1C"), "model": s.get("model", "K1C"),
            "reconnects": reconnects, "busy": busy, "busy_reason": busy_reason,
            "nozzle_temp": to_int(s.get("nozzleTemp", 0)),
            "nozzle_target": to_int(s.get("targetNozzleTemp", 0)),
            "bed_temp": to_int(s.get("bedTemp0", 0)),
            "bed_target": to_int(s.get("targetBedTemp0", 0)),
            "box_temp": s.get("boxTemp"),
            "max_nozzle": to_int(s.get("maxNozzleTemp", 320)),
            "max_bed": to_int(s.get("maxBedTemp", 115)),
            "progress": to_int(s.get("printProgress", 0)),
            "layer": to_int(s.get("layer", 0)) if active else 0,
            "total_layer": to_int(s.get("Total Layer", s.get("TotalLayer", 0))) if active else 0,
            "print_time": fmt_dur(s.get("printJobTime", 0)) if active else "--",
            "time_left": fmt_dur(s.get("printLeftTime", 0)) if active else "--",
            "state": state_key(raw_state), "state_raw": raw_state,
            "model_fan": s.get("modelFanPct"), "case_fan": s.get("caseFanPct"),
            "aux_fan": s.get("auxiliaryFanPct"), "light": s.get("lightSw"),
            "feedrate": s.get("curFeedratePct"), "flowrate": s.get("curFlowratePct"),
            "position": s.get("curPosition", ""),
            "filename": name, "errcode": to_int(err_obj.get("errcode", 0)), "err": err,
        }
    if h.get("ok"):
        return {
            "transport": "http", "link_state": "fallback", "hostname": "K1C", "model": "K1C",
            "reconnects": reconnects, "busy": busy, "busy_reason": busy_reason,
            "nozzle_temp": h.get("nozzle_temp", 0), "nozzle_target": h.get("nozzle_target", 0),
            "bed_temp": h.get("bed_temp", 0), "bed_target": h.get("bed_target", 0),
            "box_temp": None, "max_nozzle": 320, "max_bed": 115,
            "progress": h.get("progress", 0), "layer": 0, "total_layer": 0,
            "print_time": fmt_dur(h.get("job_time", 0)), "time_left": "--",
            "state": state_key(h.get("state_raw", 0)), "state_raw": h.get("state_raw", 0),
            "model_fan": None, "case_fan": None, "aux_fan": None,
            "light": None, "feedrate": None, "flowrate": None, "position": "",
            "filename": "", "errcode": 0, "err": err or h.get("err", ""),
        }
    return {
        "transport": "offline", "link_state": "offline", "hostname": "--", "model": "K1C",
        "reconnects": reconnects, "busy": busy, "busy_reason": busy_reason,
        "nozzle_temp": 0, "nozzle_target": 0, "bed_temp": 0, "bed_target": 0,
        "box_temp": None, "max_nozzle": 320, "max_bed": 115,
        "progress": 0, "layer": 0, "total_layer": 0,
        "print_time": "--", "time_left": "--", "state": "unknown", "state_raw": -1,
        "model_fan": None, "case_fan": None, "aux_fan": None,
        "light": None, "feedrate": None, "flowrate": None, "position": "",
        "filename": "", "errcode": 0, "err": err or h.get("err", ""),
    }


class K1CControlPage(orca.pages.PagesPluginCapabilityBase):
    def get_name(self):
        return "K1C Hub"

    def get_ui(self):
        lang = resolve_ui_lang(self)

        translations = {
            "en": {
                "title": "K1C Hub", "tab_control": "Control", "tab_files": "Files",
                "tab_history": "History", "tab_camera": "Camera",
                "sub_main": "Dashboard", "sub_calib": "Calibration",
                "ws": "WebSocket", "http": "HTTP fallback", "offline": "Offline",
                "reconnecting": "reconnecting", "sound_on": "Sound on", "sound_off": "Muted",
                "telemetry": "Telemetry", "auto": "AUTO",
                "nozzle": "Nozzle", "bed": "Bed", "chamber": "Chamber", "target": "target",
                "layer": "Layer", "elapsed": "Elapsed", "left": "Remaining", "flow": "Flow",
                "standby": "Standby", "printing": "Printing", "paused": "Paused",
                "complete": "Complete", "error": "Error", "unknown": "Unknown",
                "calibrating": "Calibrating",
                "no_job": "No active job", "no_job_sub": "Pick a file from the Files tab",
                "browse": "Browse files", "pause": "Pause", "resume": "Resume", "stop": "Stop",
                "stop_title": "Stop the current print?",
                "stop_body": "The job will be cancelled and the toolhead will park. This cannot be undone.",
                "temps": "Temperature", "fans": "Cooling",
                "motion": "Motion", "light_sec": "Light", "now": "now", "position": "Position",
                "model_fan": "Model fan", "case_fan": "Chassis fan", "aux_fan": "Side fan",
                "apply": "Set", "on": "ON", "off": "OFF",
                "mode_silent": "Silent", "mode_stable": "Stable", "mode_standard": "Standard",
                "mode_hyper": "Hyper", "current": "Current", "custom": "custom",
                "silent_val": "Silent value",
                "silent_hint": "Not published by Creality — set it to match your printer screen.",
                "busy_note": "Print or calibration in progress — the printer owns the heaters and the extruder. Manual control is locked.",
                "search": "Search files", "sort_date": "Newest", "sort_name": "Name",
                "sort_size": "Size", "sort_time": "Duration", "all": "All materials",
                "refresh": "Reload", "grid": "Grid", "list": "List", "files": "files",
                "selected": "selected", "select_all": "Select all", "clear": "Clear",
                "print": "Print", "delete": "Delete", "size": "Size", "layer_h": "Layer height",
                "date": "Created", "filament": "Filament", "est_time": "Print time",
                "slicer": "Slicer", "path": "Path", "material": "Material",
                "no_preview": "No preview", "pick_file": "Select a file to inspect it",
                "print_title": "Start printing", "calibrate": "Run auto-calibration first",
                "start": "Start print", "cancel": "Cancel", "busy": "Printer is busy",
                "deleted": "deleted", "undo": "Undo", "restored": "restored",
                "jobs": "Jobs", "total_time": "Print time", "total_fil": "Filament used",
                "finished": "Finished", "failed": "Interrupted", "timelapse": "Timelapse",
                "video_fail": "Video cannot be played here", "no_history": "No print history yet",
                "remove_entry": "Remove entry", "media_missing": "Media source not found yet",
                "media_find": "Find media source", "media_finding": "Searching media source",
                "cam_source": "Source", "cam_detect": "Detect", "cam_detecting": "Detecting sources",
                "cam_custom": "Custom URL", "cam_mode": "Transport", "cam_auto": "Auto",
                "cam_webrtc": "WebRTC (native)", "cam_direct": "MJPEG direct", "cam_relay": "MJPEG relay",
                "cam_fps": "Relay frame rate", "cam_view": "View", "cam_zoom": "Zoom",
                "cam_rot": "Rotation", "cam_fliph": "Flip H", "cam_flipv": "Flip V",
                "cam_image": "Image", "cam_bright": "Brightness", "cam_contrast": "Contrast",
                "cam_sat": "Saturation", "cam_overlay": "Overlays & actions",
                "cam_grid": "Rule of thirds", "cam_hud": "Telemetry HUD",
                "cam_reset": "Reset view", "cam_snap": "Snapshot", "cam_power": "Stream",
                "cam_state": "State", "cam_res": "Resolution", "cam_frames_stat": "Frames dec/recv",
                "cam_uptime": "Uptime", "cam_snaps": "Snapshots", "cam_view_btn": "View",
                "cam_back": "Back to live", "cam_nosnaps": "No snapshots yet",
                "cam_connecting": "Connecting to camera", "cam_idle": "Stream is off",
                "cam_live": "Live", "cam_error": "Camera error", "cam_nosource": "No source configured",
                "cam_saved": "Snapshot saved", "cam_saving": "Saving snapshot",
                "cam_direct_hint": "WebRTC is negotiated through the plugin, media flows straight from the printer.",
                "rtc_port": "WebRTC port", "rtc_unsupported": "WebView has no RTCPeerConnection",
                "rtc_failed": "ICE failed — no media route to the printer",
                "rtc_noframes": "ICE is up but no frames decoded — see log for SDP and codec",
                "rtc_state": "ICE", "rtc_nosnap": "Snapshot needs MJPEG mode",
                "rtc_restart": "Restart", "rtc_h264": "Prefer H.264",
                "rtc_diag": "Log diagnostics",
                "homed": "Homed", "axis_lock": "Motion lock",
                "locked": "LOCKED", "unlocked": "FREE",
                "lock_tip": "Motion lock — click to allow manual moves. Blocked while printing.",
                "step_xy": "XY step", "step_z": "Z step", "speed_xy": "XY speed", "speed_z": "Z speed",
                "xy_move": "Toolhead & bed", "z_up": "Bed up", "z_down": "Bed down",
                "z_hint": "Z− brings the bed closer to the nozzle, Z+ lowers it.",
                "home_all": "Home all", "home_xy": "Home XY", "home_z": "Home Z",
                "park_z": "Bed to bottom", "center_xy": "Center head", "motors_off": "Motors off",
                "motors_title": "Disable all stepper motors?",
                "motors_body": "Homing will be lost and the bed may drop under its own weight. Re-home before moving again.",
                "z_offset": "Z offset / babystep", "z_reset": "Reset offset",
                "limits": "Machine limits", "max_vel": "Max velocity", "max_accel": "Max accel",
                "idle_state": "Klipper state",
                "extrude": "Extrude", "retract": "Retract", "load_fil": "Load", "unload_fil": "Unload",
                "fil_len": "Length", "fil_speed": "Feed rate", "can_extrude": "Ready to extrude",
                "cannot_extrude": "Nozzle too cold", "heating": "Heating",
                "heat_hint": "Heat the nozzle above the minimum extrusion temperature.",
                "quick_heat": "Quick heat", "cooldown": "Cooldown", "flow_factor": "Flow factor",
                "e_pos": "Extruder position", "extruder_sec": "Extruder", "flow_sec": "Flow factor",
                "heat_to": "Heat to",
                "bed_mesh": "Bed mesh", "mesh_clear": "Clear mesh", "shaper": "Input shaping",
                "pid_nozzle": "PID nozzle", "pid_bed": "PID bed",
                "save_config": "Save config", "save_title": "Write printer.cfg and restart?",
                "save_body": "SAVE_CONFIG stores calibration results permanently and restarts Klipper. Do not run it during a print.",
                "macros": "Printer macros", "macros_search": "Filter macros",
                "macros_none": "No macros discovered yet", "caps_refresh": "Rescan",
                "caps_summary": "objects / macros", "run": "Run", "run_title": "Run this macro?",
                "run_body": "The macro may heat the toolhead and move the axes. Make sure the printer is idle and clear.",
                "calib_warn": "Calibration moves the toolhead and heats the bed or nozzle. Never start it during a print.",
                "standard_calib": "Standard routines", "homing_sec": "Homing & position",
                "runner_idle": "Queue idle", "runner_busy": "Running", "not_available": "not available",
                "diag": "Diagnostics", "ip": "Printer IP", "port": "WS port", "wspath": "WS path",
                "save": "Save & reconnect", "reconnect": "Reconnect", "test": "Test HTTP",
                "fallback": "Moonraker fallback", "rawws": "Raw WS JSON", "send": "Send",
                "gcode": "G-code", "clear_log": "Clear log", "sending": "Sending",
                "sent": "Sent via", "fail": "Failed", "na": "--", "done": "done",
                "err_code": "Error code", "reconnects": "reconnects",
                "url_test": "Test URL", "media": "Media",
            },
            "ru": {
                "title": "K1C Hub", "tab_control": "Управление", "tab_files": "Файлы",
                "tab_history": "История", "tab_camera": "Камера",
                "sub_main": "Панель", "sub_calib": "Калибровки",
                "ws": "WebSocket", "http": "Резерв HTTP", "offline": "Нет связи",
                "reconnecting": "переподключение", "sound_on": "Звук вкл", "sound_off": "Без звука",
                "telemetry": "Телеметрия", "auto": "АВТО",
                "nozzle": "Сопло", "bed": "Стол", "chamber": "Камера", "target": "цель",
                "layer": "Слой", "elapsed": "Прошло", "left": "Осталось", "flow": "Поток",
                "standby": "Ожидание", "printing": "Печать", "paused": "Пауза",
                "complete": "Завершено", "error": "Ошибка", "unknown": "Неизвестно",
                "calibrating": "Калибровка",
                "no_job": "Нет активной задачи", "no_job_sub": "Выбери файл на вкладке «Файлы»",
                "browse": "Открыть файлы", "pause": "Пауза", "resume": "Продолжить", "stop": "Стоп",
                "stop_title": "Остановить текущую печать?",
                "stop_body": "Задача будет отменена, голова уйдёт в парковку. Отменить это действие нельзя.",
                "temps": "Температура", "fans": "Охлаждение",
                "motion": "Движение", "light_sec": "Подсветка", "now": "сейчас", "position": "Позиция",
                "model_fan": "Обдув модели", "case_fan": "Корпусной", "aux_fan": "Боковой",
                "apply": "Задать", "on": "ВКЛ", "off": "ВЫКЛ",
                "mode_silent": "Тихое", "mode_stable": "Стабильное", "mode_standard": "Стандартное",
                "mode_hyper": "Сверхбыстрое", "current": "Текущий", "custom": "свой",
                "silent_val": "Значение тихого",
                "silent_hint": "Creality не публикует это число — подставь то, что показывает экран принтера.",
                "busy_note": "Идёт печать или калибровка — нагревателями и экструдером управляет принтер. Ручное управление заблокировано.",
                "search": "Поиск файлов", "sort_date": "Новые", "sort_name": "Имя",
                "sort_size": "Размер", "sort_time": "Время", "all": "Все материалы",
                "refresh": "Обновить", "grid": "Плитка", "list": "Список", "files": "файлов",
                "selected": "выбрано", "select_all": "Выбрать всё", "clear": "Сбросить",
                "print": "Печать", "delete": "Удалить", "size": "Размер", "layer_h": "Высота слоя",
                "date": "Создан", "filament": "Филамент", "est_time": "Время печати",
                "slicer": "Слайсер", "path": "Путь", "material": "Материал",
                "no_preview": "Нет превью", "pick_file": "Выбери файл, чтобы посмотреть детали",
                "print_title": "Запуск печати", "calibrate": "Сначала автокалибровка",
                "start": "Начать печать", "cancel": "Отмена", "busy": "Принтер занят",
                "deleted": "удалено", "undo": "Отменить", "restored": "восстановлено",
                "jobs": "Задач", "total_time": "Время печати", "total_fil": "Израсходовано",
                "finished": "Завершена", "failed": "Прервана", "timelapse": "Таймлапс",
                "video_fail": "Видео не воспроизводится здесь", "no_history": "История печати пуста",
                "remove_entry": "Удалить запись", "media_missing": "Источник медиа ещё не найден",
                "media_find": "Найти источник медиа", "media_finding": "Ищу источник медиа",
                "cam_source": "Источник", "cam_detect": "Найти", "cam_detecting": "Поиск источников",
                "cam_custom": "Свой URL", "cam_mode": "Транспорт", "cam_auto": "Авто",
                "cam_webrtc": "WebRTC (родной)", "cam_direct": "MJPEG напрямую", "cam_relay": "MJPEG через плагин",
                "cam_fps": "Кадров в релее", "cam_view": "Вид", "cam_zoom": "Масштаб",
                "cam_rot": "Поворот", "cam_fliph": "Отразить Г", "cam_flipv": "Отразить В",
                "cam_image": "Изображение", "cam_bright": "Яркость", "cam_contrast": "Контраст",
                "cam_sat": "Насыщенность", "cam_overlay": "Наложения и действия",
                "cam_grid": "Сетка третей", "cam_hud": "Телеметрия",
                "cam_reset": "Сбросить вид", "cam_snap": "Снимок", "cam_power": "Поток",
                "cam_state": "Состояние", "cam_res": "Разрешение", "cam_frames_stat": "Кадров дек/прин",
                "cam_uptime": "В эфире", "cam_snaps": "Снимки", "cam_view_btn": "Открыть",
                "cam_back": "Вернуться к эфиру", "cam_nosnaps": "Снимков пока нет",
                "cam_connecting": "Подключение к камере", "cam_idle": "Поток выключен",
                "cam_live": "Эфир", "cam_error": "Ошибка камеры", "cam_nosource": "Источник не настроен",
                "cam_saved": "Снимок сохранён", "cam_saving": "Сохраняю снимок",
                "cam_direct_hint": "WebRTC согласуется через плагин, а видео идёт напрямую с принтера.",
                "rtc_port": "Порт WebRTC", "rtc_unsupported": "В WebView нет RTCPeerConnection",
                "rtc_failed": "ICE не смог найти маршрут до принтера",
                "rtc_noframes": "ICE поднялся, но кадры не декодируются — смотри SDP и кодек в логе",
                "rtc_state": "ICE", "rtc_nosnap": "Снимок доступен только в режиме MJPEG",
                "rtc_restart": "Перезапуск", "rtc_h264": "H.264 первым",
                "rtc_diag": "Лог диагностики",
                "homed": "Дом", "axis_lock": "Блокировка движений",
                "locked": "ЗАБЛОК", "unlocked": "СВОБОДНО",
                "lock_tip": "Блокировка движений — нажми, чтобы разрешить ручное перемещение. Во время печати запрещены.",
                "step_xy": "Шаг XY", "step_z": "Шаг Z", "speed_xy": "Скорость XY", "speed_z": "Скорость Z",
                "xy_move": "Голова и стол", "z_up": "Стол вверх", "z_down": "Стол вниз",
                "z_hint": "Z− поднимает стол к соплу, Z+ опускает вниз.",
                "home_all": "Домой всё", "home_xy": "Домой XY", "home_z": "Домой Z",
                "park_z": "Стол в низ", "center_xy": "Голову в центр", "motors_off": "Отключить моторы",
                "motors_title": "Отключить все шаговые моторы?",
                "motors_body": "Позиции будут потеряны, стол может опуститься под своим весом. Перед движением нужно снова припарковать оси.",
                "z_offset": "Смещение Z / babystep", "z_reset": "Сбросить смещение",
                "limits": "Лимиты станка", "max_vel": "Макс. скорость", "max_accel": "Макс. ускорение",
                "idle_state": "Состояние Klipper",
                "extrude": "Выдавить", "retract": "Втянуть", "load_fil": "Заправить", "unload_fil": "Выгрузить",
                "fil_len": "Длина", "fil_speed": "Скорость подачи", "can_extrude": "Готов к экструзии",
                "cannot_extrude": "Сопло холодное", "heating": "Нагрев",
                "heat_hint": "Нагрей сопло выше минимальной температуры экструзии.",
                "quick_heat": "Быстрый нагрев", "cooldown": "Остудить", "flow_factor": "Множитель потока",
                "e_pos": "Позиция экструдера", "extruder_sec": "Экструдер", "flow_sec": "Множитель потока",
                "heat_to": "Нагреть до",
                "bed_mesh": "Сетка стола", "mesh_clear": "Сбросить сетку", "shaper": "Input shaping",
                "pid_nozzle": "PID сопла", "pid_bed": "PID стола",
                "save_config": "Записать конфиг", "save_title": "Записать printer.cfg и перезапустить?",
                "save_body": "SAVE_CONFIG навсегда сохранит результаты калибровки и перезапустит Klipper. Не запускай во время печати.",
                "macros": "Макросы принтера", "macros_search": "Фильтр макросов",
                "macros_none": "Макросы ещё не найдены", "caps_refresh": "Пересканировать",
                "caps_summary": "объектов / макросов", "run": "Запустить", "run_title": "Запустить макрос?",
                "run_body": "Макрос может нагреть сопло и двигать оси. Убедись, что принтер свободен и стол чистый.",
                "calib_warn": "Калибровка двигает голову и греет стол или сопло. Никогда не запускай её во время печати.",
                "standard_calib": "Стандартные процедуры", "homing_sec": "Парковка и позиция",
                "runner_idle": "Очередь пуста", "runner_busy": "Выполняется", "not_available": "недоступно",
                "diag": "Диагностика", "ip": "IP принтера", "port": "WS порт", "wspath": "WS путь",
                "save": "Сохранить", "reconnect": "Переподключить", "test": "Тест HTTP",
                "fallback": "Резерв через Moonraker", "rawws": "Сырой WS JSON", "send": "Отправить",
                "gcode": "G-code", "clear_log": "Очистить лог", "sending": "Отправка",
                "sent": "Отправлено через", "fail": "Ошибка", "na": "--", "done": "готово",
                "err_code": "Код ошибки", "reconnects": "переподключений",
                "url_test": "Проверить URL", "media": "Медиа",
            },
            "zh": {
                "title": "K1C 控制器", "tab_control": "控制", "tab_files": "文件",
                "tab_history": "历史", "tab_camera": "摄像头",
                "sub_main": "仪表板", "sub_calib": "校准",
                "ws": "WebSocket", "http": "HTTP 备用", "offline": "离线",
                "reconnecting": "重连中", "sound_on": "声音开", "sound_off": "已静音",
                "telemetry": "实时遥测", "auto": "自动",
                "nozzle": "喷嘴", "bed": "热床", "chamber": "腔体", "target": "目标",
                "layer": "层", "elapsed": "已用", "left": "剩余", "flow": "流量",
                "standby": "待机", "printing": "打印中", "paused": "暂停",
                "complete": "完成", "error": "错误", "unknown": "未知",
                "calibrating": "校准中",
                "no_job": "无进行中的任务", "no_job_sub": "在「文件」页选择文件",
                "browse": "浏览文件", "pause": "暂停", "resume": "继续", "stop": "停止",
                "stop_title": "停止当前打印？", "stop_body": "任务将被取消，喷头会归位。此操作无法撤销。",
                "temps": "温度", "fans": "散热",
                "motion": "运动", "light_sec": "照明", "now": "当前", "position": "坐标",
                "model_fan": "模型风扇", "case_fan": "机箱风扇", "aux_fan": "侧风扇",
                "apply": "设定", "on": "开", "off": "关",
                "mode_silent": "静音", "mode_stable": "均衡", "mode_standard": "标准",
                "mode_hyper": "极速", "current": "当前", "custom": "自定义",
                "silent_val": "静音数值", "silent_hint": "Creality 未公开该数值，请按打印机屏幕填写。",
                "busy_note": "正在打印或校准 — 加热器与挤出机由打印机控制，手动操作已锁定。",
                "search": "搜索文件", "sort_date": "最新", "sort_name": "名称",
                "sort_size": "大小", "sort_time": "时长", "all": "全部耗材",
                "refresh": "刷新", "grid": "网格", "list": "列表", "files": "个文件",
                "selected": "已选", "select_all": "全选", "clear": "清除",
                "print": "打印", "delete": "删除", "size": "大小", "layer_h": "层高",
                "date": "创建时间", "filament": "耗材", "est_time": "打印时长",
                "slicer": "切片软件", "path": "路径", "material": "耗材类型",
                "no_preview": "无预览", "pick_file": "选择文件查看详情",
                "print_title": "开始打印", "calibrate": "先执行自动校准",
                "start": "开始打印", "cancel": "取消", "busy": "打印机忙",
                "deleted": "已删除", "undo": "撤销", "restored": "已恢复",
                "jobs": "任务数", "total_time": "打印时长", "total_fil": "耗材消耗",
                "finished": "已完成", "failed": "已中断", "timelapse": "延时摄影",
                "video_fail": "此处无法播放视频", "no_history": "暂无打印历史",
                "remove_entry": "删除记录", "media_missing": "尚未找到媒体源",
                "media_find": "查找媒体源", "media_finding": "正在查找媒体源",
                "cam_source": "视频源", "cam_detect": "检测", "cam_detecting": "正在检测",
                "cam_custom": "自定义 URL", "cam_mode": "传输方式", "cam_auto": "自动",
                "cam_webrtc": "WebRTC（原生）", "cam_direct": "MJPEG 直连", "cam_relay": "MJPEG 中转",
                "cam_fps": "中转帧率", "cam_view": "视图", "cam_zoom": "缩放",
                "cam_rot": "旋转", "cam_fliph": "水平翻转", "cam_flipv": "垂直翻转",
                "cam_image": "图像", "cam_bright": "亮度", "cam_contrast": "对比度",
                "cam_sat": "饱和度", "cam_overlay": "叠加与操作",
                "cam_grid": "三分网格", "cam_hud": "遥测信息",
                "cam_reset": "重置视图", "cam_snap": "拍照", "cam_power": "视频流",
                "cam_state": "状态", "cam_res": "分辨率", "cam_frames_stat": "解码/接收帧",
                "cam_uptime": "运行时长", "cam_snaps": "快照", "cam_view_btn": "查看",
                "cam_back": "返回实时", "cam_nosnaps": "暂无快照",
                "cam_connecting": "正在连接", "cam_idle": "视频流已关闭", "cam_live": "实时",
                "cam_error": "摄像头错误", "cam_nosource": "未配置视频源",
                "cam_saved": "快照已保存", "cam_saving": "正在保存",
                "cam_direct_hint": "WebRTC 由插件协商，视频直接来自打印机。",
                "rtc_port": "WebRTC 端口", "rtc_unsupported": "WebView 不支持 RTCPeerConnection",
                "rtc_failed": "ICE 失败 — 无法建立到打印机的媒体通道",
                "rtc_noframes": "ICE 已连接但无法解码帧 — 请查看日志中的 SDP 与编码",
                "rtc_state": "ICE", "rtc_nosnap": "拍照仅在 MJPEG 模式可用",
                "rtc_restart": "重启", "rtc_h264": "优先 H.264",
                "rtc_diag": "诊断日志",
                "homed": "已归位", "axis_lock": "运动锁定",
                "locked": "已锁定", "unlocked": "已解锁",
                "lock_tip": "运动锁定 — 点击允许手动移动。打印中禁止。",
                "step_xy": "XY 步距", "step_z": "Z 步距", "speed_xy": "XY 速度", "speed_z": "Z 速度",
                "xy_move": "喷头与热床", "z_up": "热床上升", "z_down": "热床下降",
                "z_hint": "Z− 使热床靠近喷嘴，Z+ 使其下降。",
                "home_all": "全部归位", "home_xy": "XY 归位", "home_z": "Z 归位",
                "park_z": "热床到底", "center_xy": "喷头居中", "motors_off": "关闭电机",
                "motors_title": "关闭所有步进电机？",
                "motors_body": "将丢失归位状态，热床可能因自重下落。再次移动前需重新归位。",
                "z_offset": "Z 偏移 / 微调", "z_reset": "重置偏移", "limits": "机器限位",
                "max_vel": "最大速度", "max_accel": "最大加速度", "idle_state": "Klipper 状态",
                "extrude": "挤出", "retract": "回抽", "load_fil": "进料", "unload_fil": "退料",
                "fil_len": "长度", "fil_speed": "进料速度", "can_extrude": "可挤出",
                "cannot_extrude": "喷嘴温度过低", "heating": "加热中",
                "heat_hint": "请先将喷嘴加热到最低挤出温度以上。",
                "quick_heat": "快速加热", "cooldown": "冷却", "flow_factor": "流量系数",
                "e_pos": "挤出机位置", "extruder_sec": "挤出机", "flow_sec": "流量系数",
                "heat_to": "加热到",
                "bed_mesh": "热床网格", "mesh_clear": "清除网格", "shaper": "输入整形",
                "pid_nozzle": "喷嘴 PID", "pid_bed": "热床 PID",
                "save_config": "写入配置", "save_title": "写入 printer.cfg 并重启？",
                "save_body": "SAVE_CONFIG 会永久保存校准结果并重启 Klipper。请勿在打印中执行。",
                "macros": "打印机宏", "macros_search": "筛选宏", "macros_none": "尚未发现宏",
                "caps_refresh": "重新扫描", "caps_summary": "对象 / 宏", "run": "运行",
                "run_title": "运行此宏？", "run_body": "宏可能加热喷嘴并移动轴。请确认打印机空闲且热床清空。",
                "calib_warn": "校准会移动喷头并加热热床或喷嘴。切勿在打印中执行。",
                "standard_calib": "标准流程", "homing_sec": "归位与坐标",
                "runner_idle": "队列空闲", "runner_busy": "执行中", "not_available": "不支持",
                "diag": "诊断", "ip": "打印机 IP", "port": "WS 端口", "wspath": "WS 路径",
                "save": "保存并重连", "reconnect": "重新连接", "test": "HTTP 测试",
                "fallback": "Moonraker 备用", "rawws": "原始 WS JSON", "send": "发送",
                "gcode": "G-code", "clear_log": "清空日志", "sending": "发送中",
                "sent": "已发送 via", "fail": "失败", "na": "--", "done": "完成",
                "err_code": "错误码", "reconnects": "重连次数",
                "url_test": "测试 URL", "media": "媒体",
            },
        }
        t_json = json.dumps(translations, ensure_ascii=False)
        cam_json = json.dumps(cam_settings(), ensure_ascii=False)

        html = """<!DOCTYPE html>
<html>
<head>
<style>
:root {
  --bg: var(--orca-bg, #0b0d16); --fg: var(--orca-fg, #e9ecf8);
  --muted: var(--orca-muted, #8d95b4); --line: var(--orca-border, #232842);
  --v1: #b18cff; --v2: #7c5cff; --m1: #e05bff;
  --c1: #4fe3ff; --c2: #3b7bff; --t1: #5eead4;
  --ok: #3fe08c; --warn: #ffc247; --bad: #ff4d5e;
  --grad-pri: linear-gradient(135deg, #7c5cff, #3b7bff);
  --grad-soft: linear-gradient(135deg, #b18cff, #4fe3ff);
  --grad-noz: linear-gradient(90deg, #b18cff, #e05bff);
  --grad-bed: linear-gradient(90deg, #4fe3ff, #3b7bff);
  --grad-box: linear-gradient(90deg, #5eead4, #4fe3ff);
  --panel: rgba(255,255,255,0.038); --panel2: rgba(255,255,255,0.065);
}
* { box-sizing: border-box; }
body { margin: 0; padding: 16px 18px 26px; font-family: var(--orca-font, "Segoe UI", sans-serif);
  background: var(--bg); color: var(--fg); font-size: 13px; }
body:before { content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 0;
  background: radial-gradient(900px 430px at 6% -10%, rgba(124,92,255,0.20), transparent 62%),
    radial-gradient(780px 420px at 97% 2%, rgba(79,227,255,0.13), transparent 60%),
    radial-gradient(720px 520px at 55% 112%, rgba(224,91,255,0.10), transparent 62%); }
.wrap { position: relative; z-index: 1; }
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: rgba(255,255,255,0.04); border-radius: 8px; }
::-webkit-scrollbar-thumb { border-radius: 8px; border: 2px solid transparent; background-clip: padding-box;
  background-image: linear-gradient(180deg, #7c5cff, #4fe3ff); }
::-webkit-scrollbar-thumb:hover { background-image: linear-gradient(180deg, #b18cff, #e05bff); }
.top { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 14px; }
.brand { display: flex; align-items: center; gap: 10px; }
.mark { width: 30px; height: 30px; border-radius: 9px; padding: 0; line-height: 0; flex: none;
  display: flex; align-items: center; justify-content: center;
  background: var(--grad-soft); box-shadow: 0 6px 20px rgba(124,92,255,0.38); }
.mark svg { width: 17px; height: 17px; display: block; stroke: #0d1020; fill: none; stroke-width: 2.1; }
h1 { margin: 0; font-size: 19px; }
.pill { display: inline-flex; align-items: center; gap: 7px; font-size: 11px; font-weight: 700;
  padding: 5px 11px; border-radius: 20px; border: 1px solid transparent; }
.pill .dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
.pill.ws { color: var(--ok); background: rgba(63,224,140,0.12); border-color: rgba(63,224,140,0.32); }
.pill.ws .dot { animation: beat 2.4s infinite; }
.pill.warn { color: var(--c1); background: rgba(79,227,255,0.12); border-color: rgba(79,227,255,0.32); }
.pill.off { color: var(--bad); background: rgba(255,77,94,0.12); border-color: rgba(255,77,94,0.32); }
@keyframes beat { 0%,100% { opacity: 1; } 50% { opacity: 0.45; } }
.hostline { font-size: 11px; color: var(--muted); }
.spacer { flex: 1; }
.tabs { display: flex; gap: 4px; padding: 4px; border-radius: 12px; background: var(--panel);
  border: 1px solid var(--line); width: fit-content; margin-bottom: 12px; flex-wrap: wrap; }
.tab { padding: 8px 18px; border: none; border-radius: 9px; cursor: pointer; background: transparent;
  color: var(--muted); font-size: 13px; font-weight: 600; font-family: inherit; transition: color .18s, background .18s; }
.tab:hover { color: var(--fg); }
.tab.on { color: #fff; background: var(--grad-pri); box-shadow: 0 5px 18px rgba(90,100,255,0.34); }
.tab .count { font-size: 10px; opacity: 0.8; margin-left: 6px; }
.subbar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 14px; }
.subtabs { display: flex; gap: 3px; padding: 3px; border-radius: 10px; background: var(--panel);
  border: 1px solid var(--line); }
.sub { padding: 6px 16px; border: none; border-radius: 8px; cursor: pointer; background: transparent;
  color: var(--muted); font-size: 12px; font-weight: 700; font-family: inherit; transition: all .16s; }
.sub:hover { color: var(--fg); }
.sub.on { background: rgba(177,140,255,0.18); color: var(--v1); }
.runner { display: flex; align-items: center; gap: 8px; font-size: 11px; color: var(--muted);
  padding: 5px 11px; border-radius: 9px; background: var(--panel); border: 1px solid var(--line); }
.runner.busy { color: var(--c1); border-color: rgba(79,227,255,0.35); }
.spin { width: 9px; height: 9px; border-radius: 50%; border: 2px solid rgba(79,227,255,0.3);
  border-top-color: var(--c1); animation: spin .8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 14px; }
.ctl-grid { display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 14px; align-items: start; }
.ctl-col { display: flex; flex-direction: column; gap: 14px; min-width: 0; }
.cal-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(330px, 1fr)); gap: 14px; align-items: start; }
.sect { padding: 15px; }
.sect h2 { margin: 0 0 13px; font-size: 11px; line-height: 26px; min-height: 26px;
  text-transform: uppercase; letter-spacing: 1.4px; color: var(--muted); display: block; }
.sect h2:after { content: ""; display: block; clear: both; }
.sect h2 i.bar { display: inline-block; vertical-align: middle; width: 4px; height: 11px;
  border-radius: 3px; margin-right: 8px; position: relative; top: 4px; }
.sect h2 i.vio { background: linear-gradient(180deg, var(--v1), var(--m1)); }
.sect h2 i.cya { background: linear-gradient(180deg, var(--c1), var(--c2)); }
.sect h2 i.tea { background: linear-gradient(180deg, var(--t1), var(--c1)); }
.sect h2 .spacer { display: none; }
.sect h2 .locksq { float: right; margin-left: 6px; }
.sect h2 .locklbl, .sect h2 .autochip { float: right; margin-left: 6px; margin-top: 6px; line-height: 1; }
.sect h2 .hostline { float: right; margin-left: 6px; line-height: 26px; }
.autochip { font-size: 9px; font-weight: 800; letter-spacing: 1px; padding: 3px 8px; border-radius: 5px;
  background: rgba(255,194,71,0.16); color: var(--warn); border: 1px solid rgba(255,194,71,0.34); }

.telemetry { display: flex; align-items: stretch; flex-wrap: wrap; margin-bottom: 14px;
  border-radius: 13px; overflow: hidden; border: 1px solid rgba(177,140,255,0.32);
  background: linear-gradient(115deg, rgba(124,92,255,0.20), rgba(59,123,255,0.11) 48%, rgba(79,227,255,0.15));
  box-shadow: 0 10px 28px rgba(0,0,0,0.30), inset 0 1px 0 rgba(255,255,255,0.06); }
.telemetry .tbrand { display: flex; align-items: center; gap: 8px; padding: 0 15px; flex: none;
  background: rgba(7,9,17,0.42); border-right: 1px solid rgba(177,140,255,0.24); }
.telemetry .tbrand span { font-size: 9px; font-weight: 800; letter-spacing: 1.5px;
  text-transform: uppercase; color: var(--v1); white-space: nowrap; }
.tdot { width: 7px; height: 7px; border-radius: 50%; background: var(--ok); flex: none;
  box-shadow: 0 0 0 3px rgba(63,224,140,0.16); animation: beat 2.4s infinite; }
.tcell { padding: 9px 16px; border-right: 1px solid rgba(177,140,255,0.16); min-width: 104px; flex: 1; }
.tcell:last-child { border-right: none; }
.tcell.grow { flex: 2.4; min-width: 230px; }
.tcell > span { display: block; font-size: 8.5px; text-transform: uppercase; letter-spacing: 1.1px;
  color: var(--muted); margin-bottom: 3px; }
.tcell b { font-size: 15px; font-weight: 800; color: var(--c1); }
.tcell b.hot { color: var(--v1); }
.posline { display: flex; gap: 14px; align-items: baseline; flex-wrap: wrap; }
.positem { display: inline-flex; align-items: baseline; gap: 5px; }
.positem i { font-style: normal; }
.positem .axl { font-size: 10px; font-weight: 800; color: var(--v1); letter-spacing: 0.5px; }
.positem .axv { font-size: 14.5px; font-weight: 800; color: var(--fg); font-family: Consolas, monospace; }

.locksq { width: 26px; height: 26px; border-radius: 8px; border: none; cursor: pointer; flex: none;
  padding: 0; margin: 0; line-height: 0; display: flex; align-items: center; justify-content: center;
  color: #2a1c02; background: linear-gradient(140deg, #ffd166, #ffb020);
  box-shadow: 0 0 0 3px rgba(255,194,71,0.16), 0 4px 12px rgba(255,176,32,0.28);
  transition: transform .12s, box-shadow .18s, background .18s; }
.locksq.open { color: #04241a; background: linear-gradient(140deg, #6bf0ac, #3fe08c);
  box-shadow: 0 0 0 3px rgba(63,224,140,0.18), 0 4px 12px rgba(63,224,140,0.3); }
.locksq:hover { filter: brightness(1.1); }
.locksq:active { transform: scale(0.9); }
.locksq svg { width: 14px; height: 14px; display: block; margin: 0; stroke: currentColor;
  fill: none; stroke-width: 2.3; stroke-linecap: round; stroke-linejoin: round; }
.locklbl { font-size: 9px; font-weight: 800; letter-spacing: 1px; padding: 3px 7px; border-radius: 5px;
  background: rgba(255,194,71,0.16); color: var(--warn); }
.locklbl.open { background: rgba(63,224,140,0.16); color: var(--ok); }

.job { padding: 15px; display: flex; gap: 15px; align-items: center; }
.job.live { border-color: rgba(124,92,255,0.42); box-shadow: inset 0 0 70px rgba(124,92,255,0.09); }
.ringbox { position: relative; width: 104px; height: 104px; flex: none; }
.ring { width: 104px; height: 104px; transform: rotate(-90deg); display: block; }
.ring circle { fill: none; stroke-width: 10; stroke-linecap: round; }
.ring .bg { stroke: rgba(255,255,255,0.07); }
.ring .fg { stroke: url(#pgrad); transition: stroke-dashoffset .6s cubic-bezier(.3,.9,.3,1); }
.ringtxt { position: absolute; inset: 0; display: grid; place-items: center; text-align: center; }
.ringpct { font-size: 21px; font-weight: 800; }
.ringlbl { font-size: 8.5px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.9px; }
.jobinfo { min-width: 0; flex: 1; }
.jobstate { display: inline-flex; font-size: 9.5px; font-weight: 800; text-transform: uppercase;
  letter-spacing: 1.1px; padding: 3px 9px; border-radius: 6px; background: var(--panel2);
  color: var(--muted); margin-bottom: 8px; }
.jobstate.printing { background: rgba(124,92,255,0.18); color: var(--v1); }
.jobstate.paused { background: rgba(255,194,71,0.16); color: var(--warn); }
.jobstate.complete { background: rgba(63,224,140,0.16); color: var(--ok); }
.jobstate.error { background: rgba(255,77,94,0.16); color: var(--bad); }
.jobname { font-size: 13.5px; font-weight: 700; word-break: break-all; margin-bottom: 10px; line-height: 1.3; }
.jobsub { font-size: 11px; color: var(--muted); margin-bottom: 10px; word-break: break-all; }
.jobmetrics { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 12px; }
.metric .k { font-size: 9px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.8px; }
.metric .v { font-size: 14px; font-weight: 700; margin-top: 1px; }
.jobbtns { display: flex; gap: 7px; margin-top: 12px; flex-wrap: wrap; }

.btn { border: none; border-radius: 9px; padding: 9px 15px; font-size: 12.5px; font-weight: 700;
  cursor: pointer; font-family: inherit; transition: transform .12s, filter .18s;
  background: var(--panel2); color: var(--fg); display: inline-flex; align-items: center;
  justify-content: center; gap: 7px; line-height: 1.2; }
.btn:hover { filter: brightness(1.22); }
.btn:active { transform: scale(0.96); }
.btn svg { width: 14px; height: 14px; display: block; flex: none; }
.btn.pri { background: var(--grad-pri); color: #fff; box-shadow: 0 5px 16px rgba(90,100,255,0.3); }
.btn.cool { background: linear-gradient(135deg, var(--c1), var(--c2)); color: #06121c; }
.btn.danger { background: rgba(255,77,94,0.15); color: var(--bad); border: 1px solid rgba(255,77,94,0.4); }
.btn.ghost { background: var(--panel2); border: 1px solid var(--line); }
.btn.sm { padding: 6px 12px; font-size: 11.5px; border-radius: 7px; }
.btn.wide { width: 100%; }
.btn:disabled { opacity: 0.32; cursor: not-allowed; transform: none; filter: none; }

.gauge { margin-bottom: 8px; }
.gtop { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 5px; }
.gname { font-size: 11.5px; color: var(--muted); }
.gval { font-size: 15px; font-weight: 700; }
.gval small { font-size: 10.5px; color: var(--muted); font-weight: 500; margin-left: 5px; }
.gtrack { height: 6px; border-radius: 4px; background: rgba(255,255,255,0.07); overflow: hidden; }
.gfill { height: 100%; border-radius: 4px; transition: width .5s ease; }
.gfill.noz { background: var(--grad-noz); }
.gfill.bed { background: var(--grad-bed); }
.gfill.box { background: var(--grad-box); }
.row { margin-bottom: 12px; }
.row:last-child { margin-bottom: 0; }
.rlabel { display: flex; justify-content: space-between; align-items: baseline;
  font-size: 11px; color: var(--muted); margin-bottom: 6px; gap: 6px; }
.rlabel b { color: var(--fg); font-size: 11.5px; }
.inp { padding: 7px 10px; border: 1px solid var(--line); border-radius: 8px; background: rgba(0,0,0,0.26);
  color: var(--fg); font-size: 13px; font-family: inherit; outline: none; transition: border-color .18s; }
.inp:focus { border-color: var(--v1); }
.inp.full { width: 100%; }
.inp:disabled { opacity: 0.4; cursor: not-allowed; }
select.inp { cursor: pointer; }
.temprow { display: flex; gap: 7px; align-items: center; margin-bottom: 15px; justify-content: flex-start; }
.temp-inp { width: 66px; flex: none; text-align: center; padding: 7px 4px; font-weight: 700; }
.unit { font-size: 11px; color: var(--muted); flex: none; margin-right: 3px; }
.nowv { flex: none; min-width: 40px; text-align: center; font-size: 12px; color: var(--c1); font-weight: 800; }
.nowv em { display: block; font-size: 8px; color: var(--muted); font-style: normal;
  font-weight: 600; text-transform: uppercase; letter-spacing: 0.6px; }
.temprow .btn { flex: none; }
input[type=range] { -webkit-appearance: none; appearance: none; width: 100%; height: 5px; border-radius: 4px;
  background: rgba(255,255,255,0.1); outline: none; }
input[type=range]::-webkit-slider-thumb { -webkit-appearance: none; width: 15px; height: 15px; border-radius: 50%;
  background: linear-gradient(140deg, var(--c1), var(--c2)); cursor: pointer;
  box-shadow: 0 0 0 3px rgba(79,227,255,0.16); }
input[type=range].vio::-webkit-slider-thumb { background: linear-gradient(140deg, var(--v1), var(--v2));
  box-shadow: 0 0 0 3px rgba(177,140,255,0.18); }
.sliderrow { display: flex; align-items: center; gap: 10px; }
.toggle { min-width: 50px; padding: 6px 9px; border-radius: 7px; border: 1px solid var(--line);
  background: rgba(0,0,0,0.26); color: var(--muted); font-size: 10.5px; font-weight: 800; cursor: pointer;
  font-family: inherit; transition: all .18s; }
.toggle.on { background: linear-gradient(135deg, var(--c1), var(--c2)); color: #06121c; border-color: transparent; }
.toggle.pri.on { background: var(--grad-pri); color: #fff; }
.pair { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.triple { display: grid; grid-template-columns: repeat(3, 1fr); gap: 7px; }
.modes { display: grid; grid-template-columns: repeat(2, 1fr); gap: 7px; }
.mode { padding: 8px 5px; border-radius: 10px; border: 1px solid var(--line); cursor: pointer;
  background: rgba(0,0,0,0.26); color: var(--muted); font-family: inherit; text-align: center; transition: all .16s; }
.mode b { display: block; font-size: 11px; font-weight: 700; }
.mode small { display: block; font-size: 9.5px; opacity: 0.85; margin-top: 2px; }
.mode:hover { border-color: var(--v1); color: var(--fg); }
.mode.on { background: var(--grad-pri); border-color: transparent; color: #fff; }
.silentrow { display: flex; align-items: center; gap: 7px; margin-top: 11px;
  padding-top: 11px; border-top: 1px solid var(--line); font-size: 10.5px; color: var(--muted); }
.silentrow .inp { width: 60px; padding: 5px 8px; font-size: 12px; }
.hint { font-size: 9.5px; color: var(--muted); line-height: 1.45; margin-top: 8px; opacity: 0.85; }
.chips { display: flex; gap: 5px; flex-wrap: wrap; }
.chip { padding: 5px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; cursor: pointer;
  border: 1px solid var(--line); background: var(--panel); color: var(--muted); font-family: inherit; transition: all .16s; }
.chip:hover:not(:disabled) { color: var(--fg); border-color: var(--v1); }
.chip.on { background: var(--grad-pri); color: #fff; border-color: transparent; }
.chip.sm { padding: 4px 8px; font-size: 10.5px; }
.chip:disabled { opacity: 0.32; cursor: not-allowed; }

.jico { display: block; width: 30px; height: 30px; flex: none; overflow: visible; }
.jico path { fill: none; stroke: currentColor; stroke-width: 2.2; stroke-linecap: round; stroke-linejoin: round;
transition: stroke-width .14s ease; }
.jico .jbed { opacity: .42; stroke-width: 2; }
.jnudge { transition: transform .18s cubic-bezier(.34,1.4,.64,1); }
.jogpad .jog { font-size: 0; }
.jogpad .jog.home { font-size: 14px; letter-spacing: .5px; }
.jogpad .jog:not(.home) .jico { filter: drop-shadow(0 2px 7px rgba(177,140,255,0.32)); }
.jogpad .jog:not(.home):hover:not(:disabled) .jico path { stroke-width: 2.7; }
.jogpad .jog:not(.home):hover:not(:disabled) .jnudge { transform: translateY(-2.4px); }
.jogpad .jog:not(.home):active:not(:disabled) .jnudge { transform: translateY(-0.5px); }
.zcol .zbtn { min-height: 54px; }
.zcol .zbtn span:first-child { display: flex; align-items: center; justify-content: center; font-size: 0; line-height: 0; }
.zcol .zbtn .jico { width: 26px; height: 26px; }
.zcol .zbtn:hover:not(:disabled) .jico path { stroke-width: 2.7; }
.zcol .zbtn:hover:not(:disabled) .jnudge { transform: translateY(-2px); }
.zcol .zbtn#z-down:hover:not(:disabled) .jnudge { transform: translateY(2px); }
.jogsets { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 10px; margin-top: 12px; }
.jogset { position: relative; padding: 11px 11px 12px 14px; border-radius: 12px;
background: rgba(0,0,0,0.22); border: 1px solid var(--line); overflow: hidden; }
.jogset:before { content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 3px; }
.jogset.xy { border-color: rgba(177,140,255,0.28); }
.jogset.xy:before { background: linear-gradient(180deg, var(--v1), var(--m1)); }
.jogset.zz { border-color: rgba(79,227,255,0.28); }
.jogset.zz:before { background: linear-gradient(180deg, var(--t1), var(--c1)); }
.jshead { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
.jsax { font-size: 10px; font-weight: 800; letter-spacing: 1.3px; padding: 3px 8px; border-radius: 6px; flex: none; }
.jogset.xy .jsax { background: rgba(177,140,255,0.18); color: var(--v1); }
.jogset.zz .jsax { background: rgba(79,227,255,0.16); color: var(--c1); }
.jsnow { flex: 1; min-width: 0; text-align: right; font-size: 11px; font-weight: 700; color: var(--fg);
font-family: Consolas, monospace; letter-spacing: .2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
text-shadow: 0 1px 2px rgba(0,0,0,0.45); }
.zcol .zbtn.zhome { background: var(--grad-pri); color: #fff; border-color: transparent;
font-size: 15px; font-weight: 800; letter-spacing: .6px; text-shadow: 0 1px 3px rgba(0,0,0,0.35); }
.zcol .zbtn.zhome:hover:not(:disabled) { background: var(--grad-pri); color: #fff; border-color: transparent;
filter: brightness(1.12); transform: translateY(-1px); box-shadow: 0 6px 18px rgba(177,140,255,0.30); }
.zcol .zbtn.zhome:active:not(:disabled) { transform: translateY(0) scale(0.97); }
.zcol .zbtn.zhome:disabled { filter: grayscale(0.55); }
.jogset .row { margin-bottom: 11px; }
.jogset .row:last-child { margin-bottom: 0; }
.jogset .rlabel { margin-bottom: 6px; font-size: 10.5px; }
.jogset .rlabel b { font-size: 11px; }
.jogset .chips { display: grid; gap: 5px; }
.jogset .chips.steps { grid-template-columns: repeat(3, minmax(0,1fr)); }
.jogset .chips.speeds { grid-template-columns: repeat(2, minmax(0,1fr)); }
.jogset .chip { width: 100%; min-width: 0; padding: 5px 2px; font-size: 10.5px; text-align: center; }
@media (max-width: 560px) { .jogsets { grid-template-columns: 1fr; } }
.jogwrap { display: grid; grid-template-columns: minmax(0,1fr) 108px; gap: 12px; }
.jogpad { display: grid; grid-template-columns: repeat(3, 1fr); grid-template-rows: repeat(3, 1fr);
  gap: 6px; aspect-ratio: 1 / 1; }
.jog { border: 1px solid var(--line); border-radius: 10px; background: rgba(0,0,0,0.26);
  color: var(--fg); cursor: pointer; font-size: 16px; font-family: inherit; padding: 0; line-height: 1;
  display: flex; align-items: center; justify-content: center; transition: all .14s; }
.jog:hover:not(:disabled) { border-color: var(--v1); background: rgba(177,140,255,0.14); }
.jog:active:not(:disabled) { transform: scale(0.93); background: var(--grad-pri); color: #fff; }
.jog:disabled { opacity: 0.28; cursor: not-allowed; }
.jog.home { background: var(--grad-pri); color: #fff; border-color: transparent; font-size: 11px; font-weight: 800; }
.zcol { display: flex; flex-direction: column; gap: 6px; }
.zbtn { flex: 1; border: 1px solid var(--line); border-radius: 10px; background: rgba(0,0,0,0.26);
  color: var(--fg); cursor: pointer; font-family: inherit; padding: 4px;
  display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 2px;
  font-size: 10.5px; font-weight: 800; transition: all .14s; min-height: 46px; line-height: 1.2; }
.zbtn:hover:not(:disabled) { border-color: var(--c1); background: rgba(79,227,255,0.14); }
.zbtn:active:not(:disabled) { transform: scale(0.95); }
.zbtn:disabled { opacity: 0.28; cursor: not-allowed; }
.zread { padding: 7px; border-radius: 10px; background: rgba(0,0,0,0.3); text-align: center;
  border: 1px solid var(--line); }
.zread b { display: block; font-size: 15px; font-weight: 800; color: var(--c1); }
.zread span { font-size: 8.5px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.8px; }
.zread.near b { color: var(--warn); }
.axischips { display: flex; gap: 6px; align-items: center; }
.ax { width: 25px; height: 25px; border-radius: 8px; padding: 0; line-height: 0;
  display: flex; align-items: center; justify-content: center;
  font-size: 11px; font-weight: 800; border: 1px solid var(--line); background: rgba(0,0,0,0.26); color: var(--muted); }
.ax.on { background: rgba(63,224,140,0.16); color: var(--ok); border-color: rgba(63,224,140,0.4); }
.babys { display: flex; gap: 5px; align-items: center; }
.babys .btn { padding: 6px 7px; font-size: 10.5px; }
.babys .val { flex: 1; text-align: center; font-size: 13px; font-weight: 800; color: var(--v1);
  padding: 6px; border-radius: 9px; background: rgba(0,0,0,0.3); border: 1px solid var(--line); }
.warnbox { padding: 10px 12px; border-radius: 10px; background: rgba(255,77,94,0.09);
  border: 1px solid rgba(255,77,94,0.3); font-size: 11.5px; line-height: 1.5; }
.infobox { padding: 10px 12px; border-radius: 10px; background: rgba(79,227,255,0.07);
  border: 1px solid rgba(79,227,255,0.26); font-size: 11.5px; line-height: 1.5; }
.heatbox { padding: 10px 12px; border-radius: 10px; background: rgba(177,140,255,0.09);
  border: 1px solid rgba(177,140,255,0.3); font-size: 11.5px; line-height: 1.5; }
.autobox { padding: 10px 12px; border-radius: 10px; background: rgba(255,194,71,0.09);
  border: 1px solid rgba(255,194,71,0.32); font-size: 11.5px; line-height: 1.5; color: var(--fg); }
.caliblist { display: flex; flex-direction: column; gap: 8px; }
.crow { display: flex; gap: 9px; align-items: center; padding: 10px 12px; border-radius: 10px;
  background: rgba(0,0,0,0.22); border: 1px solid var(--line); }
.crow .cn { flex: 1; min-width: 0; }
.crow .cn b { display: block; font-size: 12px; font-weight: 700; }
.crow .cn small { display: block; font-size: 9.5px; color: var(--muted); margin-top: 2px;
  font-family: Consolas, monospace; word-break: break-all; }
.crow .inp { width: 64px; padding: 5px 8px; font-size: 12px; }
.macrolist { display: flex; flex-direction: column; gap: 5px; max-height: 320px; overflow-y: auto; padding-right: 5px; }
.mrow { display: flex; align-items: center; gap: 9px; padding: 7px 10px; border-radius: 9px;
  background: rgba(0,0,0,0.2); border: 1px solid var(--line); }
.mrow.hot { border-color: rgba(177,140,255,0.35); }
.mrow .mn { flex: 1; min-width: 0; font-size: 11px; font-weight: 700; font-family: Consolas, monospace;
  word-break: break-all; }
.bar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }
.bulkbar { display: none; align-items: center; gap: 10px; padding: 9px 13px; border-radius: 10px;
  margin-bottom: 12px; background: rgba(124,92,255,0.12); border: 1px solid rgba(124,92,255,0.36); }
.bulkbar.show { display: flex; }
.bulkbar .n { font-weight: 800; color: var(--v1); }
.filelayout { display: grid; grid-template-columns: minmax(0,1fr) 296px; gap: 16px; align-items: start; }
.filescroll { max-height: 560px; overflow-y: auto; padding-right: 6px; }
.fgrid { display: grid; grid-template-columns: repeat(auto-fill, minmax(232px, 1fr)); gap: 11px; }
.fgrid.listmode { grid-template-columns: 1fr; }
.fcard { position: relative; display: flex; gap: 11px; padding: 10px; border-radius: 12px; cursor: pointer;
  background: var(--panel); border: 1px solid var(--line); transition: transform .16s, border-color .16s; }
.fcard:hover { transform: translateY(-2px); border-color: rgba(177,140,255,0.55); }
.fcard.sel { border-color: var(--v1); box-shadow: 0 0 0 1px var(--v1); }
.fcard.checked { border-color: var(--c1); }
.fgrid.listmode .fcard { padding-right: 78px; align-items: center; }
.fthumb { width: 66px; height: 66px; flex: none; border-radius: 9px; overflow: hidden;
  display: flex; align-items: center; justify-content: center; background: rgba(0,0,0,0.32); }
.fthumb img { width: 100%; height: 100%; object-fit: contain; display: block; }
.skel { width: 100%; height: 100%; background: linear-gradient(100deg, rgba(177,140,255,0.06) 30%, rgba(79,227,255,0.14) 50%, rgba(177,140,255,0.06) 70%);
  background-size: 220% 100%; animation: shim 1.3s infinite; }
@keyframes shim { from { background-position: 180% 0; } to { background-position: -80% 0; } }
.noprev { font-size: 9px; color: var(--muted); text-align: center; padding: 4px; }
.fmeta { min-width: 0; flex: 1; }
.fname { font-size: 12px; font-weight: 700; line-height: 1.32; word-break: break-all;
  max-height: 32px; overflow: hidden; margin-bottom: 5px; }
.fline { font-size: 10.5px; color: var(--muted); margin-top: 2px; }
.mtag { display: inline-block; font-size: 9px; font-weight: 800; padding: 2px 6px; border-radius: 4px;
  background: rgba(79,227,255,0.15); color: var(--c1); margin-right: 5px; }
.fcheck { position: absolute; top: 8px; right: 8px; width: 18px; height: 18px; border-radius: 5px;
  padding: 0; margin: 0; line-height: 0; display: flex; align-items: center; justify-content: center;
  border: 1px solid var(--line); background: rgba(0,0,0,0.45); font-size: 11px; color: transparent; }
.fcheck.on { background: linear-gradient(135deg, var(--c1), var(--c2)); border-color: transparent; color: #06121c; }
.fhover { position: absolute; right: 8px; bottom: 8px; display: none; gap: 5px; }
.fcard:hover .fhover { display: flex; }
.fgrid.listmode .fhover { display: flex; right: 10px; bottom: auto; top: 50%; transform: translateY(-50%); }
.fgrid.listmode .fcheck { top: 50%; transform: translateY(-50%); right: 48px; }
.iconbtn { width: 26px; height: 26px; border-radius: 7px; border: none; cursor: pointer; flex: none;
  padding: 0; margin: 0; line-height: 0; display: flex; align-items: center; justify-content: center;
  background: rgba(11,13,22,0.88); color: var(--fg); transition: filter .16s, transform .12s; }
.iconbtn:hover { filter: brightness(1.25); }
.iconbtn:active { transform: scale(0.92); }
.iconbtn svg { width: 13px; height: 13px; display: block; margin: 0; stroke: currentColor;
  fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
.iconbtn.p { background: var(--grad-pri); color: #fff; }
.iconbtn.d:hover { background: var(--bad); color: #fff; }
.detail { padding: 15px; position: sticky; top: 12px; }
.dimg { width: 100%; height: 190px; border-radius: 11px; overflow: hidden;
  display: flex; align-items: center; justify-content: center;
  background: rgba(0,0,0,0.32); margin-bottom: 13px; }
.dimg img { max-width: 100%; max-height: 100%; object-fit: contain; display: block; }
.drow { display: flex; justify-content: space-between; gap: 10px; font-size: 11.5px; padding: 6px 0;
  border-bottom: 1px solid var(--line); }
.drow:last-of-type { border-bottom: none; }
.drow span:first-child { color: var(--muted); flex: none; }
.drow span:last-child { text-align: right; word-break: break-all; font-weight: 600; }
.dactions { display: flex; gap: 8px; margin-top: 13px; }
.dactions .btn { flex: 1; }
.empty { padding: 30px 16px; text-align: center; color: var(--muted); font-size: 12px; }
.stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 11px; margin-bottom: 14px; }
.stat { padding: 14px; border-radius: 12px; background: var(--panel); border: 1px solid var(--line); }
.stat .k { font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: var(--muted); }
.stat .v { font-size: 22px; font-weight: 800; margin-top: 5px; background: var(--grad-soft);
  -webkit-background-clip: text; background-clip: text; -webkit-text-fill-color: transparent; }
.hlist { display: flex; flex-direction: column; gap: 8px; max-height: 520px; overflow-y: auto; padding-right: 6px; }
.hrow { display: flex; gap: 12px; align-items: center; padding: 11px 13px; border-radius: 11px;
  background: var(--panel); border: 1px solid var(--line); cursor: pointer; transition: border-color .16s; }
.hrow:hover { border-color: rgba(177,140,255,0.55); }
.hrow.sel { border-color: var(--v1); }
.hbadge { width: 34px; height: 34px; border-radius: 9px; flex: none; padding: 0; line-height: 0;
  display: flex; align-items: center; justify-content: center; }
.hbadge svg { width: 16px; height: 16px; display: block; stroke-width: 2.4; fill: none; }
.hbadge.ok { background: rgba(63,224,140,0.14); color: var(--ok); }
.hbadge.bad { background: rgba(255,77,94,0.14); color: var(--bad); }
.hmain { flex: 1; min-width: 0; }
.hname { font-size: 12px; font-weight: 700; word-break: break-all; }
.hsub { font-size: 10.5px; color: var(--muted); margin-top: 3px; }
.tl { font-size: 9px; font-weight: 800; padding: 3px 7px; border-radius: 5px;
  background: rgba(177,140,255,0.18); color: var(--v1); flex: none; }
video { width: 100%; border-radius: 10px; background: #000; margin-bottom: 12px; display: block; }
.cam-layout { display: grid; grid-template-columns: minmax(0,1fr) 300px; gap: 16px; align-items: start; }
.ipm-wrap { position: relative; display: inline-flex; align-items: center; }
.ipm-btn { display: inline-flex; align-items: center; gap: 8px; height: 30px; padding: 0 11px; border-radius: 999px;
border: 1px solid rgba(139,92,246,.45); background: linear-gradient(135deg, rgba(139,92,246,.22), rgba(34,211,238,.12));
color: #e8eaf0; font: inherit; font-size: 12px; font-weight: 600; letter-spacing: .2px; cursor: pointer; white-space: nowrap;
transition: border-color .16s ease, box-shadow .16s ease, transform .12s ease; }
.ipm-btn:hover { border-color: rgba(139,92,246,.9); box-shadow: 0 0 0 3px rgba(139,92,246,.15); }
.ipm-btn:active { transform: translateY(1px); }
.ipm-btn.bad { border-color: rgba(248,113,113,.65); animation: ipmPulse 1.9s ease-in-out infinite; }
@keyframes ipmPulse { 0%,100% { box-shadow: 0 0 0 0 rgba(248,113,113,.30); } 50% { box-shadow: 0 0 0 7px rgba(248,113,113,0); } }
.ipm-dot { width: 7px; height: 7px; border-radius: 50%; flex: none; background: #9aa3b2; box-shadow: 0 0 0 3px rgba(154,163,178,.16); }
.ipm-dot.ok { background: #34d399; box-shadow: 0 0 0 3px rgba(52,211,153,.18); }
.ipm-dot.bad { background: #f87171; box-shadow: 0 0 0 3px rgba(248,113,113,.18); }
.ipm-dot.warn { background: #fbbf24; box-shadow: 0 0 0 3px rgba(251,191,36,.18); }
.ipm-btn.warn { border-color: rgba(251,191,36,.60); }
.ipm-state-txt.warn { color: #fbbf24; }
.ipm-note { display: none; margin-top: 10px; padding: 9px 11px; border-radius: 9px; font-size: 10.5px;
line-height: 1.5; color: #c9cfdd; background: rgba(248,113,113,.09); border: 1px solid rgba(248,113,113,.28); }
.ipm-note.show { display: block; }
.ipm-note.warn { background: rgba(251,191,36,.09); border-color: rgba(251,191,36,.28); }
.ipm-note b { color: #e8eaf0; font-weight: 700; }
.ipm-err { margin-top: 6px; font-family: Consolas, monospace; font-size: 9.5px; color: #9aa3b2; word-break: break-all; }
.ipm-lab { font-variant-numeric: tabular-nums; max-width: 140px; overflow: hidden; text-overflow: ellipsis; }
.ipm-car { opacity: .55; font-size: 9px; }
.ipm-scrim { position: fixed; inset: 0; z-index: 9998; background: rgba(8,9,13,.45); }
.ipm-pop { position: fixed; z-index: 9999; width: 322px; max-width: calc(100vw - 24px); max-height: calc(100vh - 24px);
overflow-y: auto; padding: 16px; border-radius: 16px; border: 1px solid rgba(255,255,255,.10);
background: linear-gradient(160deg, #1b1e27 0%, #14161d 100%); box-shadow: 0 18px 50px rgba(0,0,0,.55); color: #e8eaf0; }
.ipm-h { display: flex; align-items: center; gap: 9px; margin-bottom: 5px; font-size: 13px; font-weight: 700; }
.ipm-h i { display: block; width: 3px; height: 14px; border-radius: 2px; background: linear-gradient(180deg, #8b5cf6, #22d3ee); }
.ipm-sub { font-size: 11px; line-height: 1.55; color: #9aa3b2; margin-bottom: 12px; }
.ipm-field { display: flex; gap: 8px; align-items: center; }
.ipm-inp { flex: 1 1 auto; min-width: 0; height: 36px; padding: 0 11px; border-radius: 10px;
border: 1px solid rgba(255,255,255,.12); background: rgba(0,0,0,.30); color: #e8eaf0; font: inherit; font-size: 13px;
font-variant-numeric: tabular-nums; outline: none; transition: border-color .15s ease, box-shadow .15s ease; }
.ipm-inp:focus { border-color: rgba(139,92,246,.85); box-shadow: 0 0 0 3px rgba(139,92,246,.16); }
.ipm-inp.err { border-color: rgba(248,113,113,.85); box-shadow: 0 0 0 3px rgba(248,113,113,.15); }
.ipm-go { flex: none; height: 36px; padding: 0 15px; border-radius: 10px; border: 1px solid rgba(139,92,246,.55);
background: linear-gradient(135deg, #8b5cf6, #6d5cf6); color: #fff; font: inherit; font-size: 12px; font-weight: 700;
cursor: pointer; transition: filter .15s ease; }
.ipm-go:hover { filter: brightness(1.12); }
.ipm-go:disabled { opacity: .55; cursor: default; filter: none; }
.ipm-msg { min-height: 15px; margin-top: 8px; font-size: 11px; line-height: 1.4; color: #f87171; }
.ipm-msg.ok { color: #34d399; }
.ipm-foot { display: flex; align-items: center; justify-content: space-between; gap: 10px;
margin-top: 12px; padding-top: 11px; border-top: 1px solid rgba(255,255,255,.07); }
.ipm-state { display: flex; align-items: center; gap: 7px; font-size: 11px; color: #9aa3b2; }
.ipm-state-txt.ok { color: #34d399; }
.ipm-state-txt.bad { color: #f87171; }
.ipm-cancel { padding: 4px 7px; border: none; border-radius: 6px; background: none; color: #9aa3b2;
font: inherit; font-size: 11px; cursor: pointer; }
.ipm-cancel:hover { color: #e8eaf0; background: rgba(255,255,255,.06); }
.ipm-pop::-webkit-scrollbar { width: 8px; }
.ipm-pop::-webkit-scrollbar-track { background: rgba(255,255,255,.04); border-radius: 8px; }
.ipm-pop::-webkit-scrollbar-thumb { background: rgba(139,92,246,.45); border-radius: 8px; }
.ipm-pop::-webkit-scrollbar-thumb:hover { background: rgba(139,92,246,.72); }
.cam-stage { position: relative; width: 100%; aspect-ratio: 16 / 9; border-radius: 14px; overflow: hidden;
  background: radial-gradient(circle at 50% 40%, #131a2c, #070911 70%); border: 1px solid var(--line); }
.cam-stage img, .cam-stage video { position: absolute; inset: 0; width: 100%; height: 100%;
  object-fit: contain; transform-origin: 50% 50%; transition: filter .18s ease; background: transparent;
  margin: 0; border-radius: 0; }
.cam-grid { position: absolute; inset: 0; pointer-events: none; display: none; }
.cam-grid.on { display: block; }
.cam-grid i { position: absolute; background: rgba(177,140,255,0.34); }
.cam-grid i.v { top: 0; bottom: 0; width: 1px; }
.cam-grid i.h { left: 0; right: 0; height: 1px; }
.cam-grid u { position: absolute; left: 50%; top: 50%; width: 26px; height: 26px; margin: -13px 0 0 -13px;
  border: 1px solid rgba(79,227,255,0.6); border-radius: 50%; }
.cam-hud { position: absolute; inset: 0; pointer-events: none; display: none; font-size: 10.5px; font-weight: 700; }
.cam-hud.on { display: block; }
.cam-hud div { position: absolute; padding: 5px 9px; border-radius: 7px; background: rgba(7,9,17,0.62);
  border: 1px solid rgba(177,140,255,0.22); color: #dfe4f7; }
.cam-hud .tl2 { left: 10px; top: 10px; }
.cam-hud .tr { right: 10px; top: 10px; display: flex; align-items: center; gap: 7px; }
.cam-hud .bl { left: 10px; bottom: 10px; }
.cam-hud .br { right: 10px; bottom: 10px; }
.rec { width: 8px; height: 8px; border-radius: 50%; background: var(--bad); animation: beat 1.4s infinite; }
.cam-msg { position: absolute; inset: 0; display: grid; place-items: center; text-align: center;
  background: rgba(7,9,17,0.72); }
.cam-msg.hide { display: none; }
.cam-msg .inner { max-width: 340px; padding: 20px; }
.cam-msg .ico { width: 46px; height: 46px; margin: 0 auto 12px; border-radius: 14px; padding: 0; line-height: 0;
  display: flex; align-items: center; justify-content: center; background: var(--grad-pri); }
.cam-msg .ico svg { width: 22px; height: 22px; display: block; stroke: #fff; fill: none; stroke-width: 1.9; }
.cam-msg .ttl { font-size: 14px; font-weight: 800; margin-bottom: 6px; }
.cam-msg .sub { font-size: 11.5px; color: var(--muted); line-height: 1.5; margin-bottom: 14px; word-break: break-all; }
.cam-stats { display: grid; grid-template-columns: repeat(5, 1fr); gap: 9px; margin-top: 12px; }
.cstat { padding: 10px 11px; border-radius: 10px; background: var(--panel); border: 1px solid var(--line); }
.cstat .k { font-size: 9px; text-transform: uppercase; letter-spacing: 0.9px; color: var(--muted); }
.cstat .v { font-size: 14px; font-weight: 800; margin-top: 3px; color: var(--c1); }
.snaps { margin-top: 12px; padding: 14px; }
.snaplist { display: flex; flex-direction: column; gap: 6px; max-height: 190px; overflow-y: auto; }
.snaprow { display: flex; align-items: center; gap: 10px; padding: 8px 10px; border-radius: 9px;
  background: rgba(0,0,0,0.22); border: 1px solid var(--line); }
.snaprow .nm { flex: 1; min-width: 0; font-size: 11px; font-weight: 600; word-break: break-all; }
.snaprow .mt { font-size: 10px; color: var(--muted); }
.snapico { width: 26px; height: 26px; border-radius: 7px; flex: none; padding: 0; line-height: 0;
  display: flex; align-items: center; justify-content: center;
  background: rgba(177,140,255,0.16); color: var(--v1); }
.snapico svg { width: 13px; height: 13px; display: block; stroke: currentColor; fill: none; stroke-width: 2; }
.side { display: flex; flex-direction: column; gap: 14px; }
.diag { display: none; margin-top: 16px; padding: 15px; }
.diag.show { display: block; }
.dline { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 9px; }
.dlabel { font-size: 10.5px; color: var(--muted); min-width: 84px; text-transform: uppercase; }
.dlog { height: 220px; overflow: auto; background: rgba(0,0,0,0.34); border-radius: 10px; padding: 11px;
  font-size: 10.5px; white-space: pre-wrap; word-break: break-all; font-family: Consolas, monospace;
  color: var(--muted); border: 1px solid var(--line); }
.footer { margin-top: 16px; }
.toasts { position: fixed; right: 18px; bottom: 18px; z-index: 900; display: flex; flex-direction: column;
  gap: 8px; align-items: flex-end; }
.toast { min-width: 250px; max-width: 400px; padding: 12px 14px; border-radius: 11px; font-size: 12.5px;
  background: #151a2b; border: 1px solid var(--line); color: var(--fg); box-shadow: 0 14px 34px rgba(0,0,0,0.5);
  animation: slidein .26s cubic-bezier(.2,.9,.3,1); overflow: hidden; position: relative; }
@keyframes slidein { from { opacity: 0; transform: translateX(26px); } to { opacity: 1; transform: none; } }
.toast.ok { border-left: 3px solid var(--ok); }
.toast.err { border-left: 3px solid var(--bad); }
.toast.info { border-left: 3px solid var(--v1); }
.toast .trow { display: flex; align-items: center; gap: 12px; }
.toast .tmsg { flex: 1; word-break: break-all; }
.toast .tundo { border: none; background: transparent; color: var(--c1); font-weight: 800; cursor: pointer;
  font-size: 12px; font-family: inherit; }
.toast .tbar { position: absolute; left: 0; bottom: 0; height: 2px; background: var(--grad-soft); }
.scrim { position: fixed; inset: 0; background: rgba(6,7,16,0.74); z-index: 950; display: none;
  align-items: center; justify-content: center; padding: 20px; }
.scrim.show { display: flex; animation: fade .2s; }
@keyframes fade { from { opacity: 0; } to { opacity: 1; } }
.modal { width: 440px; max-width: 100%; border-radius: 16px; background: #141828; border: 1px solid var(--line);
  box-shadow: 0 26px 70px rgba(0,0,0,0.62); overflow: hidden; animation: pop .24s cubic-bezier(.2,.9,.3,1); }
@keyframes pop { from { opacity: 0; transform: scale(0.94) translateY(10px); } to { opacity: 1; transform: none; } }
.mhead { padding: 16px 18px; border-bottom: 1px solid var(--line); font-size: 15px; font-weight: 800;
  display: flex; align-items: center; gap: 10px; }
.mhead .mi { width: 26px; height: 26px; border-radius: 8px; flex: none; padding: 0; line-height: 0;
  display: flex; align-items: center; justify-content: center; background: var(--grad-pri); }
.mhead .mi svg { width: 14px; height: 14px; display: block; stroke: #fff; fill: none; stroke-width: 2.2; }
.mhead.warn .mi { background: linear-gradient(135deg, var(--bad), #b0306a); }
.mbody { padding: 18px; font-size: 12.5px; line-height: 1.55; }
.mfile { display: flex; gap: 12px; align-items: center; padding: 11px; border-radius: 11px;
  background: rgba(0,0,0,0.3); margin-bottom: 14px; }
.mfile .fthumb { width: 54px; height: 54px; }
.mname { font-size: 12.5px; font-weight: 700; word-break: break-all; }
.mmeta { font-size: 10.5px; color: var(--muted); margin-top: 4px; word-break: break-all; white-space: pre-wrap; }
.check { display: flex; align-items: center; gap: 10px; padding: 11px 12px; border-radius: 10px;
  background: rgba(79,227,255,0.07); border: 1px solid rgba(79,227,255,0.26); cursor: pointer; }
.check .box { width: 18px; height: 18px; border-radius: 5px; border: 1px solid var(--c1); flex: none;
  padding: 0; line-height: 0; display: flex; align-items: center; justify-content: center;
  color: transparent; font-size: 12px; }
.check.on .box { background: linear-gradient(135deg, var(--c1), var(--c2)); color: #06121c; border-color: transparent; }
.check .ct { font-size: 12px; }
.check .cs { font-size: 10.5px; color: var(--muted); margin-top: 2px; }
.mfoot { padding: 14px 18px; border-top: 1px solid var(--line); display: flex; gap: 9px; justify-content: flex-end; }
@media (max-width: 1500px) { .ctl-grid { grid-template-columns: repeat(2, minmax(0,1fr)); } }
@media (max-width: 1180px) {
  .filelayout, .cam-layout { grid-template-columns: 1fr; }
  .detail { position: static; }
  .cam-stats { grid-template-columns: repeat(2, 1fr); }
  .telemetry .tbrand { width: 100%; padding: 7px 15px; border-right: none;
    border-bottom: 1px solid rgba(177,140,255,0.24); }
}
@media (max-width: 820px) {
  .ctl-grid { grid-template-columns: 1fr; }
  .stats { grid-template-columns: 1fr; }
  .tcell { min-width: 50%; }
}
.block-h { display: flex; align-items: center; }
.block-h .dot { vertical-align: middle; }
</style>
</head>
<body>
<svg width="0" height="0" style="position:absolute"><defs>
<linearGradient id="pgrad" x1="0" y1="0" x2="1" y2="1">
<stop offset="0%" stop-color="#b18cff"/><stop offset="55%" stop-color="#7c5cff"/><stop offset="100%" stop-color="#4fe3ff"/>
</linearGradient></defs></svg>

<div class="wrap">
<div class="top">
  <div class="brand">
    <div class="mark"><svg viewBox="0 0 24 24"><path d="M12 2L3 7v10l9 5 9-5V7z"/><path d="M3 7l9 5 9-5M12 12v10"/></svg></div>
    <h1 id="t-title">K1C Hub</h1>
  </div>
  <span class="pill off" id="pill"><span class="dot"></span><span id="pill-txt">Offline</span></span>
  <span class="hostline" id="hostline"></span>
  <span class="spacer"></span>
  <button class="btn ghost sm" id="btn-sound">Sound</button>
  <button class="btn ghost sm" id="btn-diag">Diagnostics</button>
</div>

<div class="tabs">
  <button class="tab on" id="tab-control">Control</button>
  <button class="tab" id="tab-files">Files<span class="count" id="cnt-files"></span></button>
  <button class="tab" id="tab-history">History<span class="count" id="cnt-hist"></span></button>
  <button class="tab" id="tab-camera">Camera</button>
</div>

<div class="subbar" id="subbar">
  <div class="subtabs">
    <button class="sub on" id="sub-main">Dashboard</button>
    <button class="sub" id="sub-calib">Calibration</button>
  </div>
  <span class="spacer"></span>
  <div class="runner" id="runner"><span id="runner-txt">idle</span></div>
</div>

<div id="view-control">
  <div id="ctl-main">
    <div class="telemetry">
      <div class="tbrand"><span class="tdot"></span><span id="tl-title">Live telemetry</span></div>
      <div class="tcell"><span id="tl-noz">Nozzle</span><b class="hot" data-ms="noz">--</b></div>
      <div class="tcell"><span id="tl-bed">Bed</span><b data-ms="bed">--</b></div>
      <div class="tcell grow"><span id="tl-pos">Position</span><div class="posline" data-tel="pos"></div></div>
      <div class="tcell"><span id="tl-state">State</span><b data-ms="state">--</b></div>
    </div>

    <div class="ctl-grid">
      <div class="ctl-col">
        <div class="card job" id="jobcard">
          <div class="ringbox">
            <svg class="ring" viewBox="0 0 132 132">
              <circle class="bg" cx="66" cy="66" r="57"/>
              <circle class="fg" id="ringfg" cx="66" cy="66" r="57" stroke-dasharray="358.1" stroke-dashoffset="358.1"/>
            </svg>
            <div class="ringtxt"><div><div class="ringpct" id="ringpct">0%</div><div class="ringlbl" id="ringlbl">idle</div></div></div>
          </div>
          <div class="jobinfo">
            <div class="jobstate" id="jobstate">Standby</div>
            <div class="jobname" id="jobname">No active job</div>
            <div class="jobsub" id="jobsub"></div>
            <div class="jobmetrics">
              <div class="metric"><div class="k" id="k-layer">Layer</div><div class="v" id="v-layer">--</div></div>
              <div class="metric"><div class="k" id="k-elapsed">Elapsed</div><div class="v" id="v-elapsed">--</div></div>
              <div class="metric"><div class="k" id="k-left">Remaining</div><div class="v" id="v-left">--</div></div>
              <div class="metric"><div class="k" id="k-pos">Position</div><div class="v" id="v-pos" style="font-size:11px">--</div></div>
            </div>
            <div class="jobbtns" id="jobbtns"></div>
          </div>
        </div>

        <div class="card sect" id="temp-card">
          <h2><i class="bar vio"></i><span id="t-temps">Temperature</span>
            <span class="spacer"></span><span class="autochip" id="temp-auto" style="display:none">AUTO</span></h2>
          <div class="gauge">
            <div class="gtop"><span class="gname" id="g-nozzle">Nozzle</span>
              <span class="gval"><span id="gv-nozzle">--</span><small id="gt-nozzle"></small></span></div>
            <div class="gtrack"><div class="gfill noz" id="gf-nozzle" style="width:0%"></div></div>
          </div>
          <div class="temprow">
            <input type="number" class="inp temp-inp" id="in-nozzle" value="0" min="0" max="320">
            <span class="unit">&deg;C</span>
            <span class="nowv"><span id="now-nozzle">--</span><em id="now-lbl-1">now</em></span>
            <button class="btn pri sm" id="set-nozzle">Set</button>
          </div>
          <div class="gauge">
            <div class="gtop"><span class="gname" id="g-bed">Bed</span>
              <span class="gval"><span id="gv-bed">--</span><small id="gt-bed"></small></span></div>
            <div class="gtrack"><div class="gfill bed" id="gf-bed" style="width:0%"></div></div>
          </div>
          <div class="temprow">
            <input type="number" class="inp temp-inp" id="in-bed" value="0" min="0" max="115">
            <span class="unit">&deg;C</span>
            <span class="nowv"><span id="now-bed">--</span><em id="now-lbl-2">now</em></span>
            <button class="btn pri sm" id="set-bed">Set</button>
          </div>
          <div class="gauge" style="margin-bottom:0">
            <div class="gtop"><span class="gname" id="g-chamber">Chamber</span>
              <span class="gval"><span id="gv-chamber">--</span></span></div>
            <div class="gtrack"><div class="gfill box" id="gf-chamber" style="width:0%"></div></div>
          </div>
        </div>

        <div class="card sect">
          <h2><i class="bar tea"></i><span id="t-motion">Motion</span></h2>
          <div class="modes">
            <button class="mode" id="mm-silent"><b>Silent</b><small>25%</small></button>
            <button class="mode" id="mm-stable"><b>Stable</b><small>50%</small></button>
            <button class="mode" id="mm-standard"><b>Standard</b><small>100%</small></button>
            <button class="mode" id="mm-hyper"><b>Hyper</b><small>125%</small></button>
          </div>
          <div class="rlabel" style="margin-top:12px"><span id="l-motion-cur">Current</span><b id="motion-cur">--</b></div>
          <div class="silentrow">
            <span id="l-silent">Silent value</span>
            <input type="number" class="inp" id="silent-val" min="10" max="100" step="5" value="__SILENT__">
            <span>%</span>
          </div>
          <div class="hint" id="silent-hint"></div>
        </div>

        <div class="card sect">
          <h2><i class="bar vio"></i><span id="t-light">Light</span></h2>
          <button class="toggle pri" id="tg-light" style="width:100%">OFF</button>
        </div>
      </div>

      <div class="ctl-col">
        <div class="card sect">
          <h2><i class="bar vio"></i><span id="t-extruder">Extruder</span>
            <span class="spacer"></span><span class="hostline">E <b data-ms="e2">--</b></span></h2>
          <div id="extrude-badge" class="infobox"></div>
          <button class="btn pri sm wide" id="btn-heat-now" style="margin-top:8px;display:none">Heat</button>
          <div class="row" style="margin-top:13px">
            <div class="rlabel"><span id="l-fil-len">Length</span><b id="v-fil-len">10 mm</b></div>
            <div class="chips" id="chips-fil-len"></div>
          </div>
          <div class="row">
            <div class="rlabel"><span id="l-fil-speed">Feed rate</span><b id="v-fil-speed">5 mm/s</b></div>
            <div class="chips" id="chips-fil-speed"></div>
          </div>
          <div class="pair" style="margin-top:13px">
            <button class="btn pri" id="btn-extrude">Extrude</button>
            <button class="btn ghost" id="btn-retract">Retract</button>
          </div>
          <div class="pair" style="margin-top:8px">
            <button class="btn cool sm" id="btn-load">Load</button>
            <button class="btn ghost sm" id="btn-unload">Unload</button>
          </div>
          <div class="hint" id="fil-hint"></div>
        </div>

        <div class="card sect">
          <h2><i class="bar cya"></i><span id="t-quickheat">Quick heat</span>
            <span class="spacer"></span><span class="autochip" id="heat-auto" style="display:none">AUTO</span></h2>
          <div class="gauge" style="margin-bottom:11px">
            <div class="gtop"><span class="gname" id="qh-noz">Nozzle</span>
              <span class="gval"><span data-ms="noz2">--</span><small id="qh-tgt"></small></span></div>
            <div class="gtrack"><div class="gfill noz" id="qh-fill" style="width:0%"></div></div>
          </div>
          <div class="chips" id="chips-heat"></div>
          <button class="btn ghost sm wide" id="btn-cooldown" style="margin-top:10px">Cooldown</button>
        </div>

        <div class="card sect">
          <h2><i class="bar cya"></i><span id="t-fans">Cooling</span></h2>
          <div class="row">
            <div class="rlabel"><span id="l-model">Model fan</span><b id="v-model">--</b></div>
            <div class="sliderrow"><input type="range" id="s-model" min="0" max="100" value="0">
              <button class="toggle" id="tg-model">OFF</button></div>
          </div>
          <div class="row">
            <div class="rlabel"><span id="l-case">Chassis fan</span><b id="v-case">--</b></div>
            <div class="sliderrow"><input type="range" id="s-case" min="0" max="100" value="0">
              <button class="toggle" id="tg-case">OFF</button></div>
          </div>
          <div class="row">
            <div class="rlabel"><span id="l-aux">Side fan</span><b id="v-aux">--</b></div>
            <div class="sliderrow"><input type="range" id="s-aux" min="0" max="100" value="0">
              <button class="toggle" id="tg-aux">OFF</button></div>
          </div>
        </div>
      </div>

      <div class="ctl-col">
        <div class="card sect">
          <h2><i class="bar cya"></i><span id="t-xy">Toolhead &amp; bed</span>
            <span class="spacer"></span>
            <button class="locksq" id="lock-sq-1"></button>
            <span class="locklbl" id="lock-lbl">LOCKED</span>
          </h2>
          <div class="jogwrap">
            <div class="jogpad" id="jogpad">
              <button class="jog" data-dx="-1" data-dy="1">&#8598;</button>
              <button class="jog" data-dx="0" data-dy="1">&#8593;</button>
              <button class="jog" data-dx="1" data-dy="1">&#8599;</button>
              <button class="jog" data-dx="-1" data-dy="0">&#8592;</button>
              <button class="jog home" id="jog-home-xy">XY</button>
              <button class="jog" data-dx="1" data-dy="0">&#8594;</button>
              <button class="jog" data-dx="-1" data-dy="-1">&#8601;</button>
              <button class="jog" data-dx="0" data-dy="-1">&#8595;</button>
              <button class="jog" data-dx="1" data-dy="-1">&#8600;</button>
            </div>
            <div class="zcol">
<button class="zbtn" id="z-up"><span>&#9650;</span><span id="z-up-l">Bed up</span></button>
<button class="zbtn zhome" id="jog-home-z">Z</button>
<button class="zbtn" id="z-down"><span>&#9660;</span><span id="z-down-l">Bed down</span></button>
<div class="zread" id="zread" style="display:none"><b data-ms="z2">--</b><span>Z mm</span></div>
</div>
          </div>
          <div class="hint" id="z-hint"></div>
<div class="jogsets">
<div class="jogset xy">
<div class="jshead"><span class="jsax">XY</span><span class="jsnow" id="js-xy-now">--</span></div>
<div class="row">
<div class="rlabel"><span id="l-step-xy">XY step</span><b id="v-step-xy" style="display:none">1</b></div>
<div class="chips steps" id="chips-step-xy"></div>
</div>
<div class="row">
<div class="rlabel"><span id="l-speed-xy">XY speed</span><b id="v-speed-xy" style="display:none">50</b></div>
<div class="chips speeds" id="chips-speed-xy"></div>
</div>
</div>
<div class="jogset zz">
<div class="jshead"><span class="jsax">Z</span><span class="jsnow" id="js-z-now">--</span></div>
<div class="row">
<div class="rlabel"><span id="l-step-z">Z step</span><b id="v-step-z" style="display:none">1</b></div>
<div class="chips steps" id="chips-step-z"></div>
</div>
<div class="row">
<div class="rlabel"><span id="l-speed-z">Z speed</span><b id="v-speed-z" style="display:none">10</b></div>
<div class="chips speeds" id="chips-speed-z"></div>
</div>
</div>
</div>
        </div>

        <div class="card sect">
          <h2><i class="bar vio"></i><span id="t-homing">Homing &amp; position</span>
            <span class="spacer"></span><button class="locksq" id="lock-sq-2"></button></h2>
          <div class="axischips" style="margin-bottom:12px">
            <div class="ax" id="ax-x">X</div><div class="ax" id="ax-y">Y</div><div class="ax" id="ax-z">Z</div>
            <span class="spacer"></span><span class="hostline" id="idle-state"></span>
          </div>
          <div class="triple" style="margin-bottom:8px">
            <button class="btn pri sm" id="home-all">All</button>
            <button class="btn ghost sm" id="home-xy">XY</button>
            <button class="btn ghost sm" id="home-z">Z</button>
          </div>
          <div class="pair" style="margin-bottom:8px">
            <button class="btn ghost sm" id="park-z">Bed bottom</button>
            <button class="btn ghost sm" id="center-xy">Center</button>
          </div>
          <button class="btn danger sm wide" id="motors-off">Motors off</button>
          <div class="row" style="margin-top:14px">
            <div class="rlabel"><span id="l-zoff">Z offset</span></div>
            <div class="babys">
              <button class="btn ghost sm" data-baby="-0.05">-.05</button>
              <button class="btn ghost sm" data-baby="-0.01">-.01</button>
              <div class="val" id="zoff-val">0.000</div>
              <button class="btn ghost sm" data-baby="0.01">+.01</button>
              <button class="btn ghost sm" data-baby="0.05">+.05</button>
            </div>
          </div>
          <button class="btn ghost sm wide" id="zoff-reset" style="margin-top:8px">Reset offset</button>
        </div>
      </div>
    </div>
  </div>

  <div id="ctl-calib" style="display:none">
    <div class="telemetry">
      <div class="tbrand"><span class="tdot"></span><span id="tl2-title">Live telemetry</span></div>
      <div class="tcell"><span id="tl2-noz">Nozzle</span><b class="hot" data-ms="noz">--</b></div>
      <div class="tcell"><span id="tl2-bed">Bed</span><b data-ms="bed">--</b></div>
      <div class="tcell grow"><span id="tl2-pos">Position</span><div class="posline" data-tel="pos"></div></div>
      <div class="tcell"><span id="tl2-flow">Flow</span><b data-ms="flow">--</b></div>
      <div class="tcell"><span id="tl2-state">State</span><b data-ms="state">--</b></div>
    </div>
    <div class="warnbox" id="calib-warn" style="margin-bottom:14px"></div>
    <div class="cal-grid">
      <div class="card sect">
        <h2><i class="bar vio"></i><span id="t-standard">Standard routines</span></h2>
        <div class="caliblist" id="caliblist"></div>
        <button class="btn danger sm wide" id="btn-save-config" style="margin-top:12px">Save config</button>
      </div>
      <div class="card sect">
        <h2><i class="bar cya"></i><span id="t-macros">Printer macros</span></h2>
        <div class="bar" style="margin-bottom:10px">
          <input class="inp" id="macro-q" style="flex:1;min-width:120px" placeholder="Filter">
          <button class="btn ghost sm" id="caps-refresh">Rescan</button>
        </div>
        <div class="hostline" id="caps-summary" style="margin-bottom:10px"></div>
        <div class="macrolist" id="macrolist"></div>
      </div>
      <div class="card sect">
        <h2><i class="bar tea"></i><span id="t-flow">Flow factor</span></h2>
        <div class="row">
          <div class="rlabel"><span id="l-flow">Flow factor</span><b id="v-flow">100%</b></div>
          <input type="range" class="vio" id="s-flow" min="75" max="125" step="1" value="100">
        </div>
        <div class="chips" style="margin-top:10px" id="chips-flow"></div>
        <div id="flow-rows" style="margin-top:14px"></div>
      </div>
      <div class="card sect">
        <h2><i class="bar cya"></i><span id="t-limits">Machine limits</span></h2>
        <div id="limits-rows"></div>
      </div>
    </div>
  </div>
</div>

<div id="view-files" style="display:none">
  <div class="bar">
    <input class="inp" id="f-search" style="width:210px" placeholder="Search files">
    <select class="inp" id="f-sort">
      <option value="date">Newest</option><option value="name">Name</option>
      <option value="size">Size</option><option value="time">Duration</option>
    </select>
    <button class="btn ghost sm" id="f-view">Grid</button>
    <button class="btn ghost sm" id="f-refresh">Reload</button>
    <span class="spacer"></span><span class="hostline" id="f-count"></span>
  </div>
  <div class="bar"><div class="chips" id="f-chips"></div></div>
  <div class="bulkbar" id="bulkbar">
    <span class="n" id="bulk-n">0</span><span id="bulk-lbl">selected</span>
    <span class="spacer"></span>
    <button class="btn ghost sm" id="bulk-all">Select all</button>
    <button class="btn danger sm" id="bulk-del">Delete</button>
    <button class="btn ghost sm" id="bulk-clear">Clear</button>
  </div>
  <div class="filelayout">
    <div class="filescroll"><div class="fgrid" id="fgrid"></div></div>
    <div class="card detail" id="detail"></div>
  </div>
</div>

<div id="view-history" style="display:none">
  <div class="stats">
    <div class="stat"><div class="k" id="hs-jobs">Jobs</div><div class="v" id="hv-jobs">0</div></div>
    <div class="stat"><div class="k" id="hs-time">Print time</div><div class="v" id="hv-time">0m</div></div>
    <div class="stat"><div class="k" id="hs-fil">Filament used</div><div class="v" id="hv-fil">0 m</div></div>
  </div>
  <div class="bar">
    <button class="btn ghost sm" id="h-media">Find media source</button>
    <span class="hostline" id="h-media-state"></span>
  </div>
  <div class="filelayout">
    <div class="hlist" id="hlist"></div>
    <div class="card detail" id="hdetail"></div>
  </div>
</div>

<div id="view-camera" style="display:none">
  <div class="cam-layout">
    <div>
      <div class="cam-stage" id="cam-stage">
        <img id="cam-img" alt="">
        <video id="cam-video" muted autoplay playsinline style="display:none"></video>
        <div class="cam-grid" id="cam-grid">
          <i class="v" style="left:33.33%"></i><i class="v" style="left:66.66%"></i>
          <i class="h" style="top:33.33%"></i><i class="h" style="top:66.66%"></i><u></u>
        </div>
        <div class="cam-hud" id="cam-hud">
          <div class="tl2" id="hud-host">K1C</div>
          <div class="tr"><span class="rec"></span><span id="hud-state">LIVE</span></div>
          <div class="bl" id="hud-tech">--</div>
          <div class="br" id="hud-time">--:--:--</div>
        </div>
        <div class="cam-msg" id="cam-msg"><div class="inner">
          <div class="ico"><svg viewBox="0 0 24 24"><path d="M23 7l-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2"/></svg></div>
          <div class="ttl" id="cam-msg-ttl">Camera</div>
          <div class="sub" id="cam-msg-sub"></div>
          <button class="btn pri sm" id="cam-msg-btn">Detect</button>
        </div></div>
      </div>
      <div class="cam-stats">
        <div class="cstat"><div class="k" id="cs-state">State</div><div class="v" id="cv-state">--</div></div>
        <div class="cstat"><div class="k" id="cs-res">Resolution</div><div class="v" id="cv-res">--</div></div>
        <div class="cstat"><div class="k" id="cs-fps">ICE</div><div class="v" id="cv-fps">--</div></div>
        <div class="cstat"><div class="k" id="cs-rate">Frames</div><div class="v" id="cv-rate">--</div></div>
        <div class="cstat"><div class="k" id="cs-up">Uptime</div><div class="v" id="cv-up">--</div></div>
      </div>
      <div class="card snaps">
        <div class="rlabel" style="margin-bottom:10px">
          <span id="t-snaps" style="font-size:11px;text-transform:uppercase;letter-spacing:1.2px">Snapshots</span>
          <b id="snap-count">0</b>
        </div>
        <div class="snaplist" id="snaplist"></div>
      </div>
    </div>
    <div class="side">
      <div class="card sect">
<h2><i class="bar vio"></i><span id="t-cam-source">Source</span></h2>
<div class="infobox">WebRTC &middot; port 8000</div>
<button class="btn ghost sm wide" id="cam-restart" style="margin-top:12px">Restart</button>
<button class="btn ghost sm wide" id="rtc-diag" style="margin-top:8px">Log diagnostics</button>
<div class="hint" id="cam-mode-hint"></div>
<div style="display:none">
<div class="row">
<div class="rlabel"><span id="l-cam-mode">Transport</span></div>
<select class="inp full" id="cam-mode">
<option value="webrtc">WebRTC</option><option value="auto">Auto</option>
<option value="direct">MJPEG direct</option><option value="relay">MJPEG relay</option>
</select>
</div>
<div class="row">
<div class="rlabel"><span id="l-rtc-port">WebRTC port</span></div>
<input class="inp full" id="rtc-port" value="8000">
</div>
<button class="toggle" id="rtc-h264">H.264 first</button>
<div class="row"><select class="inp full" id="cam-src"></select></div>
<div class="row">
<div class="rlabel"><span id="l-cam-custom">Custom URL</span></div>
<input class="inp full" id="cam-url" placeholder="">
</div>
<div class="pair">
<button class="btn cool sm wide" id="cam-apply">Apply</button>
<button class="btn ghost sm wide" id="cam-detect">Detect</button>
</div>
<div class="row">
<div class="rlabel"><span id="l-cam-fps">Relay frame rate</span><b id="v-cam-fps">8</b></div>
<input type="range" class="vio" id="cam-fps" min="1" max="15" step="1" value="8">
</div>
</div>
</div>
      <div class="card sect">
        <h2><i class="bar cya"></i><span id="t-cam-view">View</span></h2>
        <div class="row">
          <div class="rlabel"><span id="l-cam-zoom">Zoom</span><b id="v-cam-zoom">100%</b></div>
          <input type="range" id="cam-zoom" min="100" max="300" step="5" value="100">
        </div>
        <div class="row">
          <div class="rlabel"><span id="l-cam-rot">Rotation</span></div>
          <select class="inp full" id="cam-rot">
            <option value="0">0&deg;</option><option value="90">90&deg;</option>
            <option value="180">180&deg;</option><option value="270">270&deg;</option>
          </select>
        </div>
        <div class="pair">
          <button class="toggle" id="cam-fliph" style="width:100%">Flip H</button>
          <button class="toggle" id="cam-flipv" style="width:100%">Flip V</button>
        </div>
      </div>
      <div class="card sect">
        <h2><i class="bar tea"></i><span id="t-cam-image">Image</span></h2>
        <div class="row">
          <div class="rlabel"><span id="l-cam-bright">Brightness</span><b id="v-cam-bright">100%</b></div>
          <input type="range" id="cam-bright" min="40" max="200" step="2" value="100">
        </div>
        <div class="row">
          <div class="rlabel"><span id="l-cam-contrast">Contrast</span><b id="v-cam-contrast">100%</b></div>
          <input type="range" id="cam-contrast" min="40" max="200" step="2" value="100">
        </div>
        <div class="row">
          <div class="rlabel"><span id="l-cam-sat">Saturation</span><b id="v-cam-sat">100%</b></div>
          <input type="range" id="cam-sat" min="0" max="220" step="2" value="100">
        </div>
      </div>
      <div class="card sect">
        <h2><i class="bar vio"></i><span id="t-cam-overlay">Overlays</span></h2>
        <div class="pair" style="margin-bottom:10px">
          <button class="toggle" id="cam-grid-tg" style="width:100%">Grid</button>
          <button class="toggle" id="cam-hud-tg" style="width:100%">HUD</button>
        </div>
        <button class="btn pri wide" id="cam-snap" style="margin-bottom:8px">Snapshot</button>
        <div class="pair">
          <button class="btn ghost sm wide" id="cam-reset">Reset</button>
          <button class="btn cool sm wide" id="cam-power">Stream</button>
        </div>
        <button class="btn ghost sm wide" id="cam-back" style="margin-top:8px;display:none">Back to live</button>
      </div>
    </div>
  </div>
</div>

<div class="card diag" id="diag">
  <div class="dline">
    <span class="dlabel" id="d-ip">Printer IP</span><input class="inp" id="c-ip" style="width:150px" value="__IP__">
    <span class="dlabel" id="d-port">WS port</span><input class="inp" id="c-port" style="width:74px" value="__PORT__">
    <span class="dlabel" id="d-path">WS path</span><input class="inp" id="c-path" style="width:110px" value="__PATH__">
    <button class="btn cool sm" id="c-save">Save</button>
  </div>
  <div class="dline">
    <label class="check" id="c-fbwrap" style="padding:7px 11px"><span class="box">&#10003;</span>
      <span class="ct" id="d-fb">Moonraker fallback</span></label>
    <button class="btn ghost sm" id="c-reconnect">Reconnect</button>
    <button class="btn ghost sm" id="c-test">Test HTTP</button>
    <span class="hostline" id="d-stats"></span>
  </div>
  <div class="dline">
    <span class="dlabel" id="d-media">Media</span>
    <button class="btn ghost sm" id="c-media">Find media source</button>
    <input class="inp" id="c-url" style="flex:1;min-width:220px" placeholder="http://ip:port/path">
    <button class="btn ghost sm" id="c-urltest">Test URL</button>
  </div>
  <div class="dline">
    <span class="dlabel" id="d-raw">Raw WS JSON</span>
    <input class="inp" id="c-raw" style="flex:1;min-width:280px" value='{"method":"get","params":{"getToken":1}}'>
    <button class="btn ghost sm" id="c-rawsend">Send</button>
  </div>
  <div class="dline">
    <span class="dlabel" id="d-gcode">G-code</span>
    <input class="inp" id="c-gcode" style="flex:1;min-width:280px" placeholder="SET_FAN_SPEED FAN=chassis_fan SPEED=0.5">
    <button class="btn ghost sm" id="c-gcodesend">Send</button>
    <button class="btn ghost sm" id="c-clear">Clear log</button>
  </div>
  <div class="dlog" id="dlog"></div>
</div>

<div class="footer"><span class="hostline" id="foot"></span></div>
</div>

<div class="toasts" id="toasts"></div>
<div class="scrim" id="scrim"><div class="modal" id="modal"></div></div>

<script>
var LANG = "__LANG__";
var T = __TRANSLATIONS__;
var RING = 358.1;
var S = {
  status: {}, motion: {}, caps: {}, runner: {}, media: {}, files: [], history: [], hstats: {},
  thumbs: {}, hidden: {}, checked: {}, sel: null, hsel: null,
  tab: 'control', sub: 'main', view: 'grid', mat: '', q: '', sort: 'date',
  cursor: 0, fv: -1, hv: -1, light: null, touchT: {nozzle:0, bed:0},
  touchF: {model:0, 'case':0, aux:0}, touchFlow: 0, diagOpen: false, fb: true,
  silent: __SILENT__, cs: __CAMCFG__,
  cam: {state:'idle', seq:0, sources:[], snaps:[], fps:0, kbps:0, uptime:0, err:'', url:''},
  camOn: true, camEff: 'off', camW: 0, camH: 0, viewSnap: null, snapSeen: 0,
  linkDownAt: 0, badgeKey: '', directTimer: null, audio: null,
  pc: null, rtcSeq: 0, rtcWaiting: false, rtcSent: false, rtcState: '', rtcStart: 0,
  rtcStats: null, rtcCodec: '', rtcLastStatsLog: 0, rtcWatch: null, rtcEnvLogged: false,
  jog: {step: 1, zstep: 1, speed: 50, zspeed: 10, lock: true, len: 10, fspeed: 5},
  macroQ: '', runSeen: 0, capsRendered: '', limSig: '', flowSig: '', calibSig: '',
  posSig: '', heatSig: ''
};
var STEPS_XY = [0.1, 1, 5, 10, 50];
var STEPS_Z = [0.05, 0.1, 1, 5, 10, 50];
var SPEEDS_XY = [10, 50, 100, 200];
var SPEEDS_Z = [2, 5, 10, 25];
var LENS = [1, 5, 10, 25, 50, 100];
var FSPEEDS = [1, 2, 5, 10];
var HEATS = [['PLA', 200], ['PETG', 240], ['ASA', 260], ['TPU', 220]];
var FLOWS = [90, 95, 100, 105, 110];

function t(k) { return (T[LANG] && T[LANG][k]) || (T.en && T.en[k]) || k; }
function el(id) { return document.getElementById(id); }
function now() { return new Date().getTime(); }
function txt(id, k) { var e = el(id); if (e) e.textContent = t(k); }
function mk(tag, cls, text) {
  var e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined && text !== null) e.appendChild(document.createTextNode(text));
  return e;
}
function svgIcon(path) {
  var s = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  s.setAttribute('viewBox', '0 0 24 24');
  s.setAttribute('fill', 'none'); s.setAttribute('stroke', 'currentColor');
  s.setAttribute('stroke-width', '2'); s.setAttribute('stroke-linecap', 'round');
  s.setAttribute('stroke-linejoin', 'round');
  var p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  p.setAttribute('d', path); s.appendChild(p);
  return s;
}
var IC_PLAY = 'M7 4l13 8-13 8z';
var IC_TRASH = 'M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13';
var IC_PAUSE = 'M8 5v14M16 5v14';
var IC_STOP = 'M6 6h12v12H6z';
var IC_CHECK = 'M4 12l5 5L20 6';
var IC_X = 'M6 6l12 12M18 6L6 18';
var IC_SHOT = 'M12 8a4 4 0 100 8 4 4 0 000-8zM3 7h4l2-3h6l2 3h4v13H3z';
var IC_WARN = 'M12 3l9 18H3zM12 9v5M12 17v1';
var IC_LOCK = 'M5 11h14v10H5zM8 11V7a4 4 0 018 0v4';
var IC_UNLOCK = 'M5 11h14v10H5zM8 11V7a4 4 0 017.5-2';
var IC_HEAT = 'M12 3s4 4 4 8a4 4 0 01-8 0c0-4 4-8 4-8z';

function send(a, d) { if (window.orca && window.orca.postMessage) window.orca.postMessage({action: a, data: d || {}}); }
function jlog(msg) { send('jslog', {msg: String(msg)}); }
function modeValues() { return {silent: S.silent, stable: 50, standard: 100, hyper: 125}; }
function isBusy() { return !!S.status.busy; }
function setMs(key, value) {
  var list = document.querySelectorAll('[data-ms="' + key + '"]'), i;
  for (i = 0; i < list.length; i++) list[i].textContent = value;
}

function shutter() {
  if (!S.cs.sound) return;
  try {
    if (!S.audio) {
      var Ctx = window.AudioContext || window.webkitAudioContext;
      if (!Ctx) return;
      S.audio = new Ctx();
    }
    var ac = S.audio, t0 = ac.currentTime;
    function click(at, freq, dur, gain) {
      var o = ac.createOscillator(), g = ac.createGain();
      o.type = 'square'; o.frequency.value = freq;
      g.gain.setValueAtTime(0.0001, at);
      g.gain.exponentialRampToValueAtTime(gain, at + 0.004);
      g.gain.exponentialRampToValueAtTime(0.0001, at + dur);
      o.connect(g); g.connect(ac.destination);
      o.start(at); o.stop(at + dur + 0.02);
    }
    click(t0, 2000, 0.028, 0.05);
    click(t0 + 0.075, 1250, 0.05, 0.04);
  } catch (e) {}
}

function applyLang() {
  txt('t-title', 'title'); txt('tab-control', 'tab_control'); txt('tab-camera', 'tab_camera');
  el('tab-files').innerHTML = ''; el('tab-files').appendChild(document.createTextNode(t('tab_files')));
  var c1 = mk('span', 'count', ''); c1.id = 'cnt-files'; el('tab-files').appendChild(c1);
  el('tab-history').innerHTML = ''; el('tab-history').appendChild(document.createTextNode(t('tab_history')));
  var c2 = mk('span', 'count', ''); c2.id = 'cnt-hist'; el('tab-history').appendChild(c2);
  txt('sub-main', 'sub_main'); txt('sub-calib', 'sub_calib');
  txt('btn-diag', 'diag'); txt('t-temps', 'temps'); txt('t-fans', 'fans');
  txt('t-motion', 'motion'); txt('t-light', 'light_sec'); txt('t-extruder', 'extruder_sec');
  txt('tl-title', 'telemetry'); txt('tl-noz', 'nozzle'); txt('tl-bed', 'bed');
  txt('tl-pos', 'position'); txt('tl-state', 'cam_state');
  txt('tl2-title', 'telemetry'); txt('tl2-noz', 'nozzle'); txt('tl2-bed', 'bed');
  txt('tl2-pos', 'position'); txt('tl2-flow', 'flow'); txt('tl2-state', 'cam_state');
  el('temp-auto').textContent = t('auto'); el('heat-auto').textContent = t('auto');
  txt('g-nozzle', 'nozzle'); txt('g-bed', 'bed'); txt('g-chamber', 'chamber');
  txt('now-lbl-1', 'now'); txt('now-lbl-2', 'now');
  txt('l-model', 'model_fan'); txt('l-case', 'case_fan'); txt('l-aux', 'aux_fan');
  txt('l-motion-cur', 'current'); txt('l-silent', 'silent_val'); txt('silent-hint', 'silent_hint');
  txt('k-layer', 'layer'); txt('k-elapsed', 'elapsed'); txt('k-left', 'left'); txt('k-pos', 'position');
  txt('set-nozzle', 'apply'); txt('set-bed', 'apply');
  txt('t-xy', 'xy_move'); txt('z-up-l', 'z_up'); txt('z-down-l', 'z_down'); txt('z-hint', 'z_hint');
  txt('l-step-xy', 'step_xy'); txt('l-speed-xy', 'speed_xy');
  txt('l-step-z', 'step_z'); txt('l-speed-z', 'speed_z');
  txt('t-homing', 'homing_sec'); txt('home-all', 'home_all'); txt('home-xy', 'home_xy');
  txt('home-z', 'home_z'); txt('park-z', 'park_z'); txt('center-xy', 'center_xy');
  txt('motors-off', 'motors_off'); txt('l-zoff', 'z_offset'); txt('zoff-reset', 'z_reset');
  txt('l-fil-len', 'fil_len'); txt('l-fil-speed', 'fil_speed');
  txt('btn-extrude', 'extrude'); txt('btn-retract', 'retract');
  txt('btn-load', 'load_fil'); txt('btn-unload', 'unload_fil'); txt('fil-hint', 'heat_hint');
  txt('t-quickheat', 'quick_heat'); txt('qh-noz', 'nozzle'); txt('btn-cooldown', 'cooldown');
  txt('t-standard', 'standard_calib'); txt('btn-save-config', 'save_config');
  txt('t-macros', 'macros'); txt('caps-refresh', 'caps_refresh'); txt('calib-warn', 'calib_warn');
  txt('t-flow', 'flow_sec'); txt('l-flow', 'flow_factor'); txt('t-limits', 'limits');
  el('macro-q').placeholder = t('macros_search');
  txt('f-refresh', 'refresh'); txt('bulk-lbl', 'selected');
  txt('bulk-all', 'select_all'); txt('bulk-del', 'delete'); txt('bulk-clear', 'clear');
  txt('hs-jobs', 'jobs'); txt('hs-time', 'total_time'); txt('hs-fil', 'total_fil');
  txt('h-media', 'media_find'); txt('c-media', 'media_find');
  txt('d-ip', 'ip'); txt('d-port', 'port'); txt('d-path', 'wspath');
  txt('c-save', 'save'); txt('d-fb', 'fallback'); txt('c-reconnect', 'reconnect');
  txt('c-test', 'test'); txt('d-raw', 'rawws'); txt('d-gcode', 'gcode'); txt('d-media', 'media');
  txt('c-rawsend', 'send'); txt('c-gcodesend', 'send'); txt('c-clear', 'clear_log');
  txt('c-urltest', 'url_test');
  txt('t-cam-source', 'cam_source'); txt('l-cam-custom', 'cam_custom');
  txt('cam-apply', 'apply'); txt('cam-detect', 'cam_detect');
  txt('l-cam-mode', 'cam_mode'); txt('l-cam-fps', 'cam_fps'); txt('cam-mode-hint', 'cam_direct_hint');
  txt('l-rtc-port', 'rtc_port'); txt('rtc-h264', 'rtc_h264');
  txt('cam-restart', 'rtc_restart'); txt('rtc-diag', 'rtc_diag');
  txt('t-cam-view', 'cam_view'); txt('l-cam-zoom', 'cam_zoom'); txt('l-cam-rot', 'cam_rot');
  txt('cam-fliph', 'cam_fliph'); txt('cam-flipv', 'cam_flipv');
  txt('t-cam-image', 'cam_image'); txt('l-cam-bright', 'cam_bright');
  txt('l-cam-contrast', 'cam_contrast'); txt('l-cam-sat', 'cam_sat');
  txt('t-cam-overlay', 'cam_overlay'); txt('cam-grid-tg', 'cam_grid'); txt('cam-hud-tg', 'cam_hud');
  txt('cam-reset', 'cam_reset'); txt('cam-back', 'cam_back'); txt('t-snaps', 'cam_snaps');
  txt('cs-state', 'cam_state'); txt('cs-res', 'cam_res'); txt('cs-fps', 'rtc_state');
  txt('cs-rate', 'cam_frames_stat'); txt('cs-up', 'cam_uptime');
  el('cam-snap').innerHTML = ''; el('cam-snap').appendChild(svgIcon(IC_SHOT));
  el('cam-snap').appendChild(document.createTextNode(' ' + t('cam_snap')));
  el('cam-msg-btn').textContent = t('cam_detect');
  var mo = el('cam-mode').options;
  mo[0].text = t('cam_webrtc'); mo[1].text = t('cam_auto');
  mo[2].text = t('cam_direct'); mo[3].text = t('cam_relay');
  el('f-search').placeholder = t('search');
  var o = el('f-sort').options;
  o[0].text = t('sort_date'); o[1].text = t('sort_name'); o[2].text = t('sort_size'); o[3].text = t('sort_time');
  el('f-view').textContent = S.view === 'grid' ? t('grid') : t('list');
  var keys = ['silent', 'stable', 'standard', 'hyper'], i;
  for (i = 0; i < keys.length; i++) el('mm-' + keys[i]).getElementsByTagName('b')[0].textContent = t('mode_' + keys[i]);
  var tg = ['model', 'case', 'aux'];
  for (i = 0; i < tg.length; i++) el('tg-' + tg[i]).textContent = parseInt(el('s-' + tg[i]).value, 10) > 0 ? t('on') : t('off');
  el('tg-light').textContent = S.light ? t('on') : t('off');
  el('btn-sound').textContent = S.cs.sound ? t('sound_on') : t('sound_off');
  renderMotion(); renderJogControls(); renderFilControls(); renderCalib(true); renderMacros(true);
  renderCamControls(); renderSnaps(); renderMediaState();
}

function fmtSize(b) {
  if (!b) return t('na');
  if (b > 1048576) return (b / 1048576).toFixed(1) + ' MB';
  return Math.round(b / 1024) + ' KB';
}
function fmtTime(s) {
  s = parseInt(s, 10) || 0;
  if (s <= 0) return t('na');
  var h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return h > 0 ? (h + 'h ' + (m < 10 ? '0' + m : m) + 'm') : (m + 'm');
}
function fmtClock(sec) {
  sec = parseInt(sec, 10) || 0;
  function p(n) { return n < 10 ? '0' + n : '' + n; }
  return p(Math.floor(sec / 3600)) + ':' + p(Math.floor((sec % 3600) / 60)) + ':' + p(sec % 60);
}
function fmtDate(ts) {
  if (!ts) return t('na');
  var d = new Date(ts * 1000);
  function p(n) { return n < 10 ? '0' + n : '' + n; }
  return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + ' ' + p(d.getHours()) + ':' + p(d.getMinutes());
}
function fmtFil(mm) { return mm ? (mm / 1000).toFixed(1) + ' m' : t('na'); }

function toast(msg, kind, undoFn, ms) {
  var box = mk('div', 'toast ' + (kind || 'info'));
  var row = mk('div', 'trow');
  row.appendChild(mk('div', 'tmsg', msg));
  var done = false;
  if (undoFn) {
    var u = mk('button', 'tundo', t('undo'));
    u.onclick = function() { if (done) return; done = true; undoFn(); if (box.parentNode) box.parentNode.removeChild(box); };
    row.appendChild(u);
  }
  box.appendChild(row);
  var life = ms || (undoFn ? 6000 : 3400);
  if (undoFn) {
    var bar = mk('div', 'tbar'); bar.style.width = '100%'; box.appendChild(bar);
    var start = now();
    var iv = setInterval(function() {
      var left = 1 - (now() - start) / life;
      if (left <= 0) { clearInterval(iv); left = 0; }
      bar.style.width = (left * 100) + '%';
    }, 60);
    setTimeout(function() { clearInterval(iv); }, life + 100);
  }
  el('toasts').appendChild(box);
  setTimeout(function() { if (!done && box.parentNode) box.parentNode.removeChild(box); }, life);
}

function closeModal() { el('scrim').className = 'scrim'; el('modal').innerHTML = ''; }
function openModal(title, icon, warn, bodyNode, okLabel, okClass, onOk) {
  var m = el('modal'); m.innerHTML = '';
  var head = mk('div', 'mhead' + (warn ? ' warn' : ''));
  var mi = mk('div', 'mi'); mi.appendChild(svgIcon(icon)); head.appendChild(mi);
  head.appendChild(mk('span', null, title)); m.appendChild(head);
  var body = mk('div', 'mbody'); body.appendChild(bodyNode); m.appendChild(body);
  var foot = mk('div', 'mfoot');
  var c = mk('button', 'btn ghost', t('cancel')); c.onclick = closeModal;
  var k = mk('button', 'btn ' + okClass, okLabel);
  k.onclick = function() { closeModal(); onOk(); };
  foot.appendChild(c); foot.appendChild(k); m.appendChild(foot);
  el('scrim').className = 'scrim show';
}
function confirmSimple(title, body, okLabel, onOk) {
  var b = mk('div'); b.appendChild(mk('div', 'warnbox', body));
  openModal(title, IC_WARN, true, b, okLabel, 'danger', onOk);
}

function thumbNode(name, big) {
  var st = S.thumbs[name];
  var box = mk('div', big ? 'dimg' : 'fthumb');
  if (st && st.uri) { var img = document.createElement('img'); img.src = st.uri; box.appendChild(img); }
  else if (st && st.uri === '') box.appendChild(mk('div', 'noprev', t('no_preview')));
  else box.appendChild(mk('div', 'skel'));
  return box;
}

function confirmPrint(f) {
  if (isBusy()) { toast(t('busy'), 'err'); return; }
  var body = mk('div');
  var fr = mk('div', 'mfile');
  fr.appendChild(thumbNode(f.name, false));
  var meta = mk('div');
  meta.appendChild(mk('div', 'mname', f.name));
  meta.appendChild(mk('div', 'mmeta', (f.material ? f.material + '  ·  ' : '') + fmtSize(f.size) + '  ·  ' + fmtTime(f.duration)));
  fr.appendChild(meta); body.appendChild(fr);
  var cal = true;
  var chk = mk('div', 'check on');
  chk.appendChild(mk('span', 'box', '\\u2713'));
  var cw = mk('div');
  cw.appendChild(mk('div', 'ct', t('calibrate')));
  cw.appendChild(mk('div', 'cs', 'enableSelfTest: 1'));
  chk.appendChild(cw);
  chk.onclick = function() { cal = !cal; chk.className = cal ? 'check on' : 'check'; };
  body.appendChild(chk);
  openModal(t('print_title'), IC_PLAY, false, body, t('start'), 'pri', function() {
    send('file_print', {path: f.path, calibrate: cal ? 1 : 0});
  });
}

function confirmStop() {
  var body = mk('div');
  body.appendChild(mk('div', 'warnbox', t('stop_body')));
  if (S.status.filename) { var n = mk('div', 'mmeta', S.status.filename); n.style.marginTop = '10px'; body.appendChild(n); }
  openModal(t('stop_title'), IC_STOP, true, body, t('stop'), 'danger', function() { send('print_cmd', {cmd: 'cancel'}); });
}

function renderLink() {
  var s = S.status, cls, label;
  if (s.transport === 'ws') {
    if (s.link_state === 'reconnecting') {
      if (!S.linkDownAt) S.linkDownAt = now();
      if (now() - S.linkDownAt > 5000) { cls = 'pill warn'; label = t('ws') + ' · ' + t('reconnecting'); }
      else { cls = 'pill ws'; label = t('ws'); }
    } else { S.linkDownAt = 0; cls = 'pill ws'; label = t('ws'); }
  } else if (s.transport === 'http') { cls = 'pill warn'; label = t('http'); }
  else { cls = 'pill off'; label = t('offline'); }
  var key = cls + '|' + label;
  if (key !== S.badgeKey) {
    el('pill').className = cls; el('pill-txt').textContent = label; S.badgeKey = key;
  }
  el('hostline').textContent = (s.hostname || '') + (s.model ? ' · ' + s.model : '');
  el('d-stats').textContent = t('reconnects') + ': ' + (s.reconnects || 0) +
    (s.errcode ? '  ·  ' + t('err_code') + ': ' + s.errcode : '');
  el('foot').textContent = s.err ? String(s.err).substring(0, 120) : '';
}

function renderRunner() {
  var r = S.runner || {}, box = el('runner');
  box.innerHTML = '';
  if (r.busy) {
    box.className = 'runner busy';
    box.appendChild(mk('span', 'spin'));
    box.appendChild(mk('span', null, t('runner_busy') + ': ' + (r.current || '')));
    if (r.queued) box.appendChild(mk('span', null, '(+' + r.queued + ')'));
  } else {
    box.className = 'runner';
    box.appendChild(mk('span', null, t('runner_idle')));
  }
}

function gauge(key, val, target, max) {
  el('gv-' + key).textContent = (val === null || val === undefined) ? t('na') : (val + '\\u00B0C');
  var pct = (val !== null && val !== undefined && max > 0) ? Math.min(100, Math.round(val / max * 100)) : 0;
  el('gf-' + key).style.width = pct + '%';
  el('gt-' + key).textContent = t('target') + ' ' + (target || 0) + '\\u00B0C';
}

function renderMotion() {
  var f = S.status.feedrate, vals = modeValues(), matched = false, k;
  for (k in vals) {
    if (!vals.hasOwnProperty(k)) continue;
    var b = el('mm-' + k);
    if (!b) continue;
    var on = (f !== null && f !== undefined && parseInt(f, 10) === vals[k]);
    b.className = on ? 'mode on' : 'mode';
    if (on) matched = true;
    b.getElementsByTagName('small')[0].textContent = vals[k] + '%';
  }
  el('motion-cur').textContent = (f === null || f === undefined) ? t('na')
    : (f + '%' + (matched ? '' : '  ·  ' + t('custom')));
}

function renderControl() {
  var s = S.status;
  renderLink();
  var active = s.state_raw === 1 || s.state_raw === 4 || isBusy();
  el('jobcard').className = 'card job' + (active ? ' live' : '');
  var pct = s.progress || 0;
  el('ringfg').setAttribute('stroke-dashoffset', String(RING * (1 - pct / 100)));
  el('ringpct').textContent = pct + '%';
  el('ringlbl').textContent = t(s.state || 'unknown');
  var stLabel = t(s.state || 'unknown');
  if (isBusy() && s.busy_reason === 'calibrating' && s.state_raw !== 1) stLabel = t('calibrating');
  el('jobstate').className = 'jobstate ' + (s.state || '');
  el('jobstate').textContent = stLabel;
  el('jobname').textContent = s.filename ? s.filename : t('no_job');
  var sub = s.filename ? '' : t('no_job_sub');
  el('jobsub').textContent = sub;
  el('jobsub').style.display = sub ? 'block' : 'none';
  el('v-layer').textContent = (s.layer > 0 && s.total_layer > 0) ? (s.layer + ' / ' + s.total_layer) : t('na');
  el('v-elapsed').textContent = s.print_time || t('na');
  el('v-left').textContent = s.time_left || t('na');
  el('v-pos').textContent = s.position ? String(s.position).replace(/X:|Y:|Z:/g, '').replace(/\\s+/g, ' ') : t('na');
  var jb = el('jobbtns'); jb.innerHTML = '';
  if (s.state_raw === 1) {
    var bp = mk('button', 'btn ghost sm'); bp.appendChild(svgIcon(IC_PAUSE));
    bp.appendChild(document.createTextNode(' ' + t('pause')));
    bp.onclick = function() { send('print_cmd', {cmd: 'pause'}); }; jb.appendChild(bp);
  }
  if (s.state_raw === 4) {
    var br = mk('button', 'btn cool sm'); br.appendChild(svgIcon(IC_PLAY));
    br.appendChild(document.createTextNode(' ' + t('resume')));
    br.onclick = function() { send('print_cmd', {cmd: 'resume'}); }; jb.appendChild(br);
  }
  if (s.state_raw === 1 || s.state_raw === 4) {
    var bs = mk('button', 'btn danger sm'); bs.appendChild(svgIcon(IC_STOP));
    bs.appendChild(document.createTextNode(' ' + t('stop')));
    bs.onclick = confirmStop; jb.appendChild(bs);
  } else if (!isBusy()) {
    var bb = mk('button', 'btn pri sm'); bb.appendChild(svgIcon(IC_PLAY));
    bb.appendChild(document.createTextNode(' ' + t('browse')));
    bb.onclick = function() { showTab('files'); }; jb.appendChild(bb);
  }
  gauge('nozzle', s.nozzle_temp, s.nozzle_target, s.max_nozzle || 320);
  gauge('bed', s.bed_temp, s.bed_target, s.max_bed || 115);
  el('gv-chamber').textContent = (s.box_temp === null || s.box_temp === undefined) ? t('na') : (s.box_temp + '\\u00B0C');
  el('gf-chamber').style.width = (s.box_temp ? Math.min(100, s.box_temp / 60 * 100) : 0) + '%';
  el('now-nozzle').textContent = (s.nozzle_temp === null || s.nozzle_temp === undefined) ? t('na') : (s.nozzle_temp + '\\u00B0');
  el('now-bed').textContent = (s.bed_temp === null || s.bed_temp === undefined) ? t('na') : (s.bed_temp + '\\u00B0');
  var busy = isBusy();
  el('temp-auto').style.display = busy ? 'inline-block' : 'none';
  el('heat-auto').style.display = busy ? 'inline-block' : 'none';
  el('in-nozzle').disabled = busy;
  el('in-bed').disabled = busy;
  el('set-nozzle').disabled = busy;
  el('set-bed').disabled = busy;
  el('btn-cooldown').disabled = busy;
  if (!busy) {
    if (now() - S.touchT.nozzle > 6000) el('in-nozzle').value = s.nozzle_target || 0;
    if (now() - S.touchT.bed > 6000) el('in-bed').value = s.bed_target || 0;
  }
  syncFan('model', s.model_fan); syncFan('case', s.case_fan); syncFan('aux', s.aux_fan);
  renderMotion();
  if (s.light !== null && s.light !== undefined) {
    S.light = s.light ? 1 : 0;
    el('tg-light').textContent = S.light ? t('on') : t('off');
    el('tg-light').className = S.light ? 'toggle pri on' : 'toggle pri';
  }
}

function syncFan(k, v) {
  if (v === null || v === undefined) { el('v-' + k).textContent = t('na'); return; }
  el('v-' + k).textContent = v + '%';
  if (now() - S.touchF[k] > 5000) el('s-' + k).value = v;
  var shown = parseInt(el('s-' + k).value, 10);
  el('tg-' + k).textContent = shown > 0 ? t('on') : t('off');
  el('tg-' + k).className = shown > 0 ? 'toggle on' : 'toggle';
}

function chipRow(boxId, values, current, suffix, onPick) {
  var box = el(boxId); box.innerHTML = '';
  var i;
  for (i = 0; i < values.length; i++) {
    (function(v) {
      var c = mk('button', 'chip sm' + (v === current ? ' on' : ''), v + suffix);
      c.onclick = function() { onPick(v); };
      box.appendChild(c);
    })(values[i]);
  }
}

function renderLock() {
  var locked = S.jog.lock, ids = ['lock-sq-1', 'lock-sq-2'], i;
  for (i = 0; i < ids.length; i++) {
    var b = el(ids[i]);
    if (!b) continue;
    b.className = locked ? 'locksq' : 'locksq open';
    b.innerHTML = '';
    b.appendChild(svgIcon(locked ? IC_LOCK : IC_UNLOCK));
    b.title = t('lock_tip');
  }
  el('lock-lbl').textContent = locked ? t('locked') : t('unlocked');
  el('lock-lbl').className = locked ? 'locklbl' : 'locklbl open';
}


function jogArrowSvg(angle) {
return '<svg class="jico" viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
'<g transform="rotate(' + angle + ' 12 12)"><g class="jnudge">' +
'<path d="M12 19 L12 5 M6.5 10.5 L12 5 L17.5 10.5"></path>' +
'</g></g></svg>';
}

function jogZSvg(up) {
var arrow = up ? 'M12 16.5 L12 5.5 M7 10.5 L12 5.5 L17 10.5'
: 'M12 7.5 L12 18.5 M7 13.5 L12 18.5 L17 13.5';
var bed = up ? 'M5.5 20.5 L18.5 20.5' : 'M5.5 3.5 L18.5 3.5';
return '<svg class="jico zico" viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
'<g class="jnudge"><path d="' + arrow + '"></path></g>' +
'<path class="jbed" d="' + bed + '"></path></svg>';
}

function jogIcons() {
var pad = el('jogpad'), list, i, b, dx, dy, a, sp;
if (pad) {
list = pad.getElementsByTagName('button');
for (i = 0; i < list.length; i++) {
b = list[i];
if (b.getAttribute('data-dx') === null) continue;
if (b.getAttribute('data-ico') === '1') continue;
dx = parseInt(b.getAttribute('data-dx'), 10) || 0;
dy = parseInt(b.getAttribute('data-dy'), 10) || 0;
a = Math.round(Math.atan2(dx, dy) * 180 / Math.PI);
if (a < 0) a += 360;
b.innerHTML = jogArrowSvg(a);
b.setAttribute('data-ico', '1');
}
}
b = el('z-up');
if (b && b.getAttribute('data-ico') !== '1') {
sp = b.getElementsByTagName('span')[0];
if (sp) { sp.innerHTML = jogZSvg(true); b.setAttribute('data-ico', '1'); }
}
b = el('z-down');
if (b && b.getAttribute('data-ico') !== '1') {
sp = b.getElementsByTagName('span')[0];
if (sp) { sp.innerHTML = jogZSvg(false); b.setAttribute('data-ico', '1'); }
}
}

var JOGZ_TITLE = {
en: 'Home Z axis (bed)',
ru: 'Домой ось Z (стол)',
zh: 'Z 轴归零（热床）'
};

/* Bind the Z home button once; refresh its tooltip on every language change. */
function jogHomeZInit() {
var b = el('jog-home-z');
if (!b) return;
b.setAttribute('title', JOGZ_TITLE[ipmLang()] || JOGZ_TITLE.en);
if (b.getAttribute('data-bound') === '1') return;
b.setAttribute('data-bound', '1');
b.onclick = function() {
if (this.disabled) return;
send('home', { axes: 'z' });
toast(t('sending'), 'info');
};
}

function renderJogControls() {
var j = S.jog;
jogIcons();
jogHomeZInit();
chipRow('chips-step-xy', STEPS_XY, j.step, '', function(v) { j.step = v; renderJogControls(); });
chipRow('chips-speed-xy', SPEEDS_XY, j.speed, '', function(v) { j.speed = v; renderJogControls(); });
chipRow('chips-step-z', STEPS_Z, j.zstep, '', function(v) { j.zstep = v; renderJogControls(); });
chipRow('chips-speed-z', SPEEDS_Z, j.zspeed, '', function(v) { j.zspeed = v; renderJogControls(); });
el('v-step-xy').textContent = j.step + ' mm';
el('v-speed-xy').textContent = j.speed + ' mm/s';
el('v-step-z').textContent = j.zstep + ' mm';
el('v-speed-z').textContent = j.zspeed + ' mm/s';
el('js-xy-now').textContent = j.step + 'mm @ ' + j.speed + 'mm/s';
el('js-z-now').textContent = j.zstep + 'mm @ ' + j.zspeed + 'mm/s';
renderLock();
updateMotionGates();
}

/* Mirror the XY home gate onto the Z home button without touching the original. */
var k1cMotionGatesBase = updateMotionGates;
updateMotionGates = function() {
var xy, z;
k1cMotionGatesBase();
xy = el('jog-home-xy');
z = el('jog-home-z');
if (z && xy) z.disabled = xy.disabled;
};

function updateMotionGates() {
  var m = S.motion || {}, j = S.jog;
  var printing = isBusy();
  var homed = String(m.homed || '');
  var canXY = !j.lock && !printing && homed.indexOf('x') >= 0 && homed.indexOf('y') >= 0;
  var canZ = !j.lock && !printing && homed.indexOf('z') >= 0;
  var pads = el('jogpad').getElementsByTagName('button'), i;
  for (i = 0; i < pads.length; i++) {
    if (pads[i].id === 'jog-home-xy') pads[i].disabled = j.lock || printing;
    else pads[i].disabled = !canXY;
  }
  el('z-up').disabled = !canZ;
  el('z-down').disabled = !canZ;
  el('home-all').disabled = j.lock || printing;
  el('home-xy').disabled = j.lock || printing;
  el('home-z').disabled = j.lock || printing;
  el('park-z').disabled = !canZ;
  el('center-xy').disabled = !canXY;
  el('motors-off').disabled = j.lock || printing;
  var ax = ['x', 'y', 'z'];
  for (i = 0; i < ax.length; i++) el('ax-' + ax[i]).className = homed.indexOf(ax[i]) >= 0 ? 'ax on' : 'ax';
}

function renderPosition(m) {
  var sig = m.ok ? (m.x.toFixed(1) + '|' + m.y.toFixed(1) + '|' + m.z.toFixed(2)) : 'na';
  if (sig === S.posSig) return;
  S.posSig = sig;
  var boxes = document.querySelectorAll('[data-tel="pos"]'), i, k;
  var axes = [['X', m.ok ? m.x.toFixed(1) : '--'],
              ['Y', m.ok ? m.y.toFixed(1) : '--'],
              ['Z', m.ok ? m.z.toFixed(2) : '--']];
  for (i = 0; i < boxes.length; i++) {
    boxes[i].innerHTML = '';
    for (k = 0; k < axes.length; k++) {
      var item = mk('span', 'positem');
      item.appendChild(mk('i', 'axl', axes[k][0]));
      item.appendChild(mk('i', 'axv', axes[k][1]));
      boxes[i].appendChild(item);
    }
  }
}

function renderMotionState() {
  var m = S.motion || {}, s = S.status;
  var noz = (m.ok && m.nozzle !== undefined) ? m.nozzle : s.nozzle_temp;
  var tgt = m.ok ? m.nozzle_target : (s.nozzle_target || 0);
  var bed = s.bed_temp;
  var busy = isBusy();
  setMs('noz', (noz === null || noz === undefined) ? t('na') : noz + '\\u00B0');
  setMs('noz2', (noz === null || noz === undefined) ? t('na') : noz + '\\u00B0C');
  setMs('bed', (bed === null || bed === undefined) ? t('na') : bed + '\\u00B0');
  setMs('e2', m.ok ? m.e.toFixed(1) : t('na'));
  setMs('flow', m.ok ? m.extrude_factor + '%' : t('na'));
  var stLabel = t(s.state || 'unknown');
  if (busy && s.busy_reason === 'calibrating' && s.state_raw !== 1) stLabel = t('calibrating');
  setMs('state', stLabel);
  renderPosition(m);
  el('zread').className = (m.ok && m.z < 5) ? 'zread near' : 'zread';
  el('zread').getElementsByTagName('b')[0].textContent = m.ok ? m.z.toFixed(2) : t('na');
  el('idle-state').textContent = m.ok ? (m.idle || '') : (m.err || '');
  el('zoff-val').textContent = m.ok ? m.z_offset.toFixed(3) : '0.000';
  el('qh-tgt').textContent = t('target') + ' ' + tgt + '\\u00B0C';
  el('qh-fill').style.width = (noz ? Math.min(100, noz / 320 * 100) : 0) + '%';

  var canE = m.ok && m.can_extrude && !busy;
  var badge = el('extrude-badge'), heatBtn = el('btn-heat-now');
  heatBtn.style.display = 'none';
  if (busy) {
    badge.className = 'autobox';
    badge.textContent = t('busy_note');
  } else if (!m.ok) {
    badge.className = 'warnbox';
    badge.textContent = m.err || t('na');
  } else if (m.can_extrude) {
    badge.className = 'infobox';
    badge.textContent = t('can_extrude') + '  ·  ' + noz + '\\u00B0C';
  } else if (tgt > 40 && noz < tgt - 2) {
    badge.className = 'heatbox';
    badge.textContent = t('heating') + '  ·  ' + noz + '\\u00B0 \\u2192 ' + tgt + '\\u00B0C';
  } else {
    badge.className = 'warnbox';
    badge.textContent = t('cannot_extrude') + '  ·  ' + noz + '\\u00B0C';
    heatBtn.style.display = 'inline-flex';
    heatBtn.innerHTML = '';
    heatBtn.appendChild(svgIcon(IC_HEAT));
    heatBtn.appendChild(document.createTextNode(' ' + t('heat_to') + ' ' + S.cs.last_heat + '\\u00B0C'));
  }
  el('btn-extrude').disabled = !canE;
  el('btn-retract').disabled = !canE;
  el('btn-load').disabled = !canE;
  el('btn-unload').disabled = !canE;

  var hsig = (busy ? 'b' : 'f') + '|' + S.cs.last_heat;
  if (hsig !== S.heatSig) {
    S.heatSig = hsig;
    var hb = el('chips-heat'); hb.innerHTML = '';
    var i;
    for (i = 0; i < HEATS.length; i++) {
      (function(pair) {
        var c = mk('button', 'chip sm', pair[0] + ' ' + pair[1] + '\\u00B0');
        c.disabled = busy;
        c.onclick = function() {
          S.cs.last_heat = pair[1];
          S.touchT.nozzle = now();
          send('control', {kind: 'nozzle', value: pair[1]});
        };
        hb.appendChild(c);
      })(HEATS[i]);
    }
  }

  if (m.ok && now() - S.touchFlow > 6000) {
    el('s-flow').value = m.extrude_factor;
    el('v-flow').textContent = m.extrude_factor + '%';
  }
  var fsig = m.ok ? (m.e.toFixed(1) + '|' + m.extrude_factor + '|' + m.abs_e) : 'na';
  if (fsig !== S.flowSig) {
    S.flowSig = fsig;
    var fr = el('flow-rows'); fr.innerHTML = '';
    if (m.ok) {
      fr.appendChild(drow(t('e_pos'), m.e.toFixed(2) + ' mm'));
      fr.appendChild(drow(t('flow_factor'), m.extrude_factor + '%'));
      fr.appendChild(drow('E mode', m.abs_e ? 'absolute' : 'relative'));
    } else { fr.appendChild(mk('div', 'empty', t('na'))); }
  }
  var lsig = m.ok ? [m.xmin, m.xmax, m.ymin, m.ymax, m.zmin, m.zmax, m.max_vel, m.max_accel, m.homed, m.idle].join('|') : (m.err || 'na');
  if (lsig !== S.limSig) {
    S.limSig = lsig;
    var lr = el('limits-rows'); lr.innerHTML = '';
    if (m.ok) {
      lr.appendChild(drow('X', m.xmin.toFixed(1) + ' … ' + m.xmax.toFixed(1) + ' mm'));
      lr.appendChild(drow('Y', m.ymin.toFixed(1) + ' … ' + m.ymax.toFixed(1) + ' mm'));
      lr.appendChild(drow('Z', m.zmin.toFixed(2) + ' … ' + m.zmax.toFixed(2) + ' mm'));
      lr.appendChild(drow(t('max_vel'), m.max_vel + ' mm/s'));
      lr.appendChild(drow(t('max_accel'), m.max_accel + ' mm/s\\u00B2'));
      lr.appendChild(drow(t('homed'), m.homed ? m.homed.toUpperCase() : '—'));
      lr.appendChild(drow(t('idle_state'), m.idle || '—'));
      lr.appendChild(drow(t('z_offset'), m.z_offset.toFixed(3) + ' mm'));
    } else { lr.appendChild(mk('div', 'empty', m.err || t('na'))); }
  }
  updateMotionGates();
}

function renderFilControls() {
  var j = S.jog;
  chipRow('chips-fil-len', LENS, j.len, '', function(v) { j.len = v; renderFilControls(); });
  chipRow('chips-fil-speed', FSPEEDS, j.fspeed, '', function(v) { j.fspeed = v; renderFilControls(); });
  el('v-fil-len').textContent = j.len + ' mm';
  el('v-fil-speed').textContent = j.fspeed + ' mm/s';
  var fb = el('chips-flow'); fb.innerHTML = '';
  var i;
  for (i = 0; i < FLOWS.length; i++) {
    (function(v) {
      var c = mk('button', 'chip sm', v + '%');
      c.onclick = function() { send('flow', {pct: v}); };
      fb.appendChild(c);
    })(FLOWS[i]);
  }
}

function calibRow(label, cmd, enabled, onRun, inputId, inputVal) {
  var row = mk('div', 'crow');
  var cn = mk('div', 'cn');
  cn.appendChild(mk('b', null, label));
  cn.appendChild(mk('small', null, enabled ? cmd : cmd + '  ·  ' + t('not_available')));
  row.appendChild(cn);
  if (inputId) {
    var inp = document.createElement('input');
    inp.type = 'number'; inp.className = 'inp'; inp.id = inputId; inp.value = inputVal;
    row.appendChild(inp);
  }
  var b = mk('button', 'btn ' + (enabled ? 'pri' : 'ghost') + ' sm', t('run'));
  b.disabled = !enabled;
  b.onclick = onRun;
  row.appendChild(b);
  return row;
}

function renderCalib(force) {
  var flags = (S.caps && S.caps.flags) || {};
  var blocked = isBusy();
  var sig = [flags.bed_mesh, flags.resonance_tester, flags.configfile, blocked].join('|');
  if (!force && sig === S.calibSig) return;
  S.calibSig = sig;
  var pn = el('pid-noz-target'), pb = el('pid-bed-target');
  var pnv = pn ? pn.value : S.cs.pid_nozzle;
  var pbv = pb ? pb.value : S.cs.pid_bed;
  var box = el('caliblist'); box.innerHTML = '';
  box.appendChild(calibRow(t('bed_mesh'), 'BED_MESH_CALIBRATE', !!flags.bed_mesh && !blocked,
    function() { send('calib', {kind: 'bed_mesh'}); }));
  box.appendChild(calibRow(t('mesh_clear'), 'BED_MESH_CLEAR', !!flags.bed_mesh && !blocked,
    function() { send('calib', {kind: 'mesh_clear'}); }));
  box.appendChild(calibRow(t('shaper'), 'SHAPER_CALIBRATE', !!flags.resonance_tester && !blocked,
    function() { send('calib', {kind: 'shaper'}); }));
  box.appendChild(calibRow(t('pid_nozzle'), 'PID_CALIBRATE HEATER=extruder', !blocked,
    function() { send('calib', {kind: 'pid_nozzle', target: el('pid-noz-target').value}); },
    'pid-noz-target', pnv));
  box.appendChild(calibRow(t('pid_bed'), 'PID_CALIBRATE HEATER=heater_bed', !blocked,
    function() { send('calib', {kind: 'pid_bed', target: el('pid-bed-target').value}); },
    'pid-bed-target', pbv));
  el('btn-save-config').disabled = !flags.configfile || blocked;
}

function renderMacros(force) {
  var caps = S.caps || {};
  var macros = caps.macros || [];
  var q = S.macroQ.toLowerCase();
  var sig = macros.join('|') + '#' + q + '#' + (isBusy() ? 'b' : 'i') + '#' + (caps.busy ? 'b' : 'r');
  if (!force && sig === S.capsRendered) return;
  S.capsRendered = sig;
  var box = el('macrolist'); box.innerHTML = '';
  el('caps-summary').textContent = (caps.objects || 0) + ' / ' + macros.length + '  ' + t('caps_summary') +
    (caps.busy ? '  ·  ' + t('cam_detecting') : '');
  el('caps-refresh').disabled = !!caps.busy;
  var shown = 0, i;
  for (i = 0; i < macros.length; i++) {
    var name = macros[i];
    if (q && name.toLowerCase().indexOf(q) < 0) continue;
    shown++;
    (function(nm) {
      var hot = /CALIB|TEST|MESH|SHAPER|PID|PROBE|LEVEL|TILT/i.test(nm);
      var row = mk('div', 'mrow' + (hot ? ' hot' : ''));
      row.appendChild(mk('div', 'mn', nm));
      var b = mk('button', 'btn ' + (hot ? 'pri' : 'ghost') + ' sm', t('run'));
      b.disabled = isBusy();
      b.onclick = function() {
        confirmSimple(t('run_title'), t('run_body') + '\\n\\n' + nm, t('run'), function() { send('macro', {name: nm}); });
      };
      row.appendChild(b);
      box.appendChild(row);
    })(name);
  }
  if (!shown) box.appendChild(mk('div', 'empty', t('macros_none')));
}

function visibleFiles() {
  var out = [], i, q = S.q.toLowerCase();
  for (i = 0; i < S.files.length; i++) {
    var f = S.files[i];
    if (S.hidden[f.name]) continue;
    if (q && f.name.toLowerCase().indexOf(q) < 0) continue;
    if (S.mat && f.material !== S.mat) continue;
    out.push(f);
  }
  out.sort(function(a, b) {
    if (S.sort === 'name') return a.name.toLowerCase() < b.name.toLowerCase() ? -1 : 1;
    if (S.sort === 'size') return b.size - a.size;
    if (S.sort === 'time') return (b.duration || 0) - (a.duration || 0);
    return b.ts - a.ts;
  });
  return out;
}

function renderChips() {
  var counts = {}, i;
  for (i = 0; i < S.files.length; i++) {
    var m = S.files[i].material;
    if (m) counts[m] = (counts[m] || 0) + 1;
  }
  var keys = [];
  for (var k in counts) { if (counts.hasOwnProperty(k)) keys.push(k); }
  keys.sort();
  var box = el('f-chips'); box.innerHTML = '';
  var all = mk('button', 'chip' + (S.mat === '' ? ' on' : ''), t('all') + ' · ' + S.files.length);
  all.onclick = function() { S.mat = ''; renderFiles(); };
  box.appendChild(all);
  for (i = 0; i < keys.length; i++) {
    (function(key) {
      var c = mk('button', 'chip' + (S.mat === key ? ' on' : ''), key + ' · ' + counts[key]);
      c.onclick = function() { S.mat = (S.mat === key ? '' : key); renderFiles(); };
      box.appendChild(c);
    })(keys[i]);
  }
}

function renderFiles() {
  renderChips();
  var list = visibleFiles();
  el('f-count').textContent = list.length + ' / ' + S.files.length + ' ' + t('files');
  el('cnt-files').textContent = S.files.length ? String(S.files.length) : '';
  var grid = el('fgrid');
  grid.className = 'fgrid' + (S.view === 'list' ? ' listmode' : '');
  grid.innerHTML = '';
  if (!list.length) grid.appendChild(mk('div', 'empty', t('na')));
  var i;
  for (i = 0; i < list.length; i++) grid.appendChild(fileCard(list[i]));
  var n = 0;
  for (var k in S.checked) { if (S.checked.hasOwnProperty(k) && S.checked[k]) n++; }
  el('bulkbar').className = n ? 'bulkbar show' : 'bulkbar';
  el('bulk-n').textContent = String(n);
  renderDetail();
}

function fileCard(f) {
  var card = mk('div', 'fcard' + (S.sel === f.name ? ' sel' : '') + (S.checked[f.name] ? ' checked' : ''));
  card.setAttribute('data-name', f.name);
  card.appendChild(thumbNode(f.name, false));
  var info = mk('div', 'fmeta');
  info.appendChild(mk('div', 'fname', f.name));
  var l1 = mk('div', 'fline');
  if (f.material) l1.appendChild(mk('span', 'mtag', f.material));
  l1.appendChild(document.createTextNode(fmtSize(f.size)));
  info.appendChild(l1);
  var bits = [];
  if (f.layer) bits.push(parseFloat(f.layer).toFixed(2) + ' mm');
  if (f.duration) bits.push(fmtTime(f.duration));
  info.appendChild(mk('div', 'fline', bits.join('  ·  ')));
  info.appendChild(mk('div', 'fline', fmtDate(f.ts)));
  card.appendChild(info);
  var chk = mk('div', 'fcheck' + (S.checked[f.name] ? ' on' : ''), '\\u2713');
  chk.onclick = function(ev) { ev.stopPropagation(); S.checked[f.name] = !S.checked[f.name]; renderFiles(); };
  card.appendChild(chk);
  var hv = mk('div', 'fhover');
  var bp = mk('button', 'iconbtn p'); bp.appendChild(svgIcon(IC_PLAY));
  bp.title = t('print');
  bp.onclick = function(ev) { ev.stopPropagation(); confirmPrint(f); };
  var bd = mk('button', 'iconbtn d'); bd.appendChild(svgIcon(IC_TRASH));
  bd.title = t('delete');
  bd.onclick = function(ev) { ev.stopPropagation(); deleteFiles([f]); };
  hv.appendChild(bp); hv.appendChild(bd); card.appendChild(hv);
  card.onclick = function() { S.sel = f.name; renderFiles(); };
  return card;
}

function drow(k, v) {
  var r = mk('div', 'drow');
  r.appendChild(mk('span', null, k)); r.appendChild(mk('span', null, v));
  return r;
}
function findFile(name) {
  for (var i = 0; i < S.files.length; i++) { if (S.files[i].name === name) return S.files[i]; }
  return null;
}

function renderDetail() {
  var box = el('detail'); box.innerHTML = '';
  var f = findFile(S.sel);
  if (!f || S.hidden[f.name]) { box.appendChild(mk('div', 'empty', t('pick_file'))); return; }
  box.appendChild(thumbNode(f.name, true));
  var meta = (S.thumbs[f.name] || {}).meta || {};
  box.appendChild(drow(t('material'), f.material || t('na')));
  box.appendChild(drow(t('size'), fmtSize(f.size)));
  box.appendChild(drow(t('layer_h'), f.layer ? parseFloat(f.layer).toFixed(2) + ' mm' : t('na')));
  box.appendChild(drow(t('est_time'), fmtTime(meta.estimated_time || f.duration)));
  box.appendChild(drow(t('filament'), fmtFil(f.filament)));
  box.appendChild(drow(t('date'), fmtDate(f.ts)));
  if (meta.slicer) box.appendChild(drow(t('slicer'), String(meta.slicer).split(' ')[0]));
  box.appendChild(drow(t('path'), f.path));
  var act = mk('div', 'dactions');
  var bp = mk('button', 'btn pri'); bp.appendChild(svgIcon(IC_PLAY));
  bp.appendChild(document.createTextNode(' ' + t('print')));
  bp.onclick = function() { confirmPrint(f); };
  var bd = mk('button', 'btn danger'); bd.appendChild(svgIcon(IC_TRASH));
  bd.onclick = function() { deleteFiles([f]); };
  act.appendChild(bp); act.appendChild(bd); box.appendChild(act);
}

function deleteFiles(arr) {
  var i, names = [];
  for (i = 0; i < arr.length; i++) {
    S.hidden[arr[i].name] = 1;
    delete S.checked[arr[i].name];
    names.push(arr[i].name);
    send('file_delete', {path: arr[i].path, name: arr[i].name});
  }
  if (S.sel && S.hidden[S.sel]) S.sel = null;
  renderFiles();
  var label = arr.length === 1 ? arr[0].name : (arr.length + ' ' + t('files'));
  toast(label + ' — ' + t('deleted'), 'info', function() {
    for (var j = 0; j < names.length; j++) delete S.hidden[names[j]];
    send('file_undo', {names: names});
    renderFiles();
    toast(label + ' — ' + t('restored'), 'ok');
  }, 6000);
}

function requestThumbs() {
  var want = [], i;
  if (S.sel && !S.thumbs[S.sel]) want.push(S.sel);
  var grid = el('fgrid'), cards = grid.children, limit = 900;
  if (grid.parentNode) limit = grid.parentNode.scrollTop + grid.parentNode.clientHeight + 320;
  for (i = 0; i < cards.length; i++) {
    if (want.length >= 12) break;
    if (cards[i].offsetTop > limit) continue;
    var n = cards[i].getAttribute('data-name');
    if (n && !S.thumbs[n] && want.indexOf(n) < 0) want.push(n);
  }
  return want;
}

function applyThumbs(th) {
  var changed = false, k;
  for (k in th) { if (!th.hasOwnProperty(k)) continue; S.thumbs[k] = th[k]; changed = true; }
  if (changed && S.tab === 'files') renderFiles();
}

var IPM_TXT = {
en: { set: 'Set printer IP', title: 'Printer connection',
sub: 'Enter the K1C IP address. On the printer: Settings > Network > Wi-Fi.',
ph: '192.168.1.50', go: 'Connect', cancel: 'Close',
bad: 'Enter a valid IPv4 address, for example 192.168.1.50',
sent: 'Connecting to ', online: 'Connected', offline: 'No connection',
reconnecting: 'Reconnecting', fallback: 'HTTP fallback', unknown: 'Waiting for status',
noteOff: 'check the IP address, make sure the printer is powered on and on the same network.',
noteWarn: 'The native WebSocket link is down. The plugin is retrying and may be using the Moonraker HTTP fallback.' },
ru: { set: 'Указать IP', title: 'Подключение к принтеру',
sub: 'Введите IP-адрес K1C. На принтере: Настройки > Сеть > Wi-Fi.',
ph: '192.168.1.50', go: 'Подключить', cancel: 'Закрыть',
bad: 'Нужен IPv4-адрес, например 192.168.1.50',
sent: 'Подключаюсь к ', online: 'Подключено', offline: 'Нет связи',
reconnecting: 'Переподключение', fallback: 'Резерв HTTP', unknown: 'Ждём статус',
noteOff: 'проверьте IP-адрес, убедитесь, что принтер включён и находится в той же сети.',
noteWarn: 'Основной канал WebSocket недоступен. Плагин переподключается и может работать через резервный Moonraker HTTP.' },
zh: { set: '设置 IP', title: '打印机连接',
sub: '输入 K1C 的 IP 地址。打印机上：设置 > 网络 > Wi-Fi。',
ph: '192.168.1.50', go: '连接', cancel: '关闭',
bad: '请输入有效的 IPv4 地址，例如 192.168.1.50',
sent: '正在连接 ', online: '已连接', offline: '未连接',
reconnecting: '重新连接中', fallback: 'HTTP 备用', unknown: '等待状态',
noteOff: '请检查 IP 地址，确认打印机已开机并处于同一网络。',
noteWarn: '原生 WebSocket 已断开。插件正在重试，可能已切换到 Moonraker HTTP 备用通道。' }
};

function ipmLang() {
var l = '', s;
try { if (LANG) l = LANG; } catch (e) {}
if (!l) { s = el('lang'); if (s && s.value) l = s.value; }
l = String(l || 'en').toLowerCase();
if (l.indexOf('ru') === 0) return 'ru';
if (l.indexOf('zh') === 0) return 'zh';
return 'en';
}

function ipmT(k) {
var d = IPM_TXT[ipmLang()] || IPM_TXT.en;
return d[k] || IPM_TXT.en[k] || k;
}

function ipmClean(v) { return String(v || '').replace(/^\s+|\s+$/g, ''); }

function ipmValid(v) {
var m = ipmClean(v).match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/), i;
if (!m) return false;
for (i = 1; i <= 4; i++) { if (parseInt(m[i], 10) > 255) return false; }
return true;
}

/* Fallback lookup for the diagnostics IP field if its id ever changes. */
function ipmField() {
var list = document.getElementsByTagName('input'), i;
for (i = 0; i < list.length; i++) {
if (list[i].id === 'ipm-inp') continue;
if (ipmValid(list[i].value)) return list[i];
}
return null;
}

function ipmCurrent() {
var f = el('c-ip');
if (f && ipmValid(f.value)) return ipmClean(f.value);
f = ipmField();
return f ? ipmClean(f.value) : '';
}

/* Real link state published by snapshot(): transport + link_state. */
function ipmLink() {
var s = {}, tr, ls;
try { s = S.status || {}; } catch (e) {}
tr = String(s.transport || '').toLowerCase();
ls = String(s.link_state || '').toLowerCase();
if (tr === 'ws') return ls === 'connected' ? 'ok' : 'warn';
if (tr === 'http') return 'warn';
if (tr === 'offline') return 'bad';
return '';
}

function ipmStateText(link) {
var s = {};
try { s = S.status || {}; } catch (e) {}
if (link === 'ok') return ipmT('online');
if (link === 'bad') return ipmT('offline');
if (link === 'warn') {
return String(s.transport || '') === 'http' ? ipmT('fallback') : ipmT('reconnecting');
}
return ipmT('unknown');
}

function ipmHost() {
var a = el('btn-sound'), sel = ['.top', '.topbar', '.header', 'header'], i, list;
if (a && a.parentNode) return { parent: a.parentNode, before: a };
for (i = 0; i < sel.length; i++) {
list = document.querySelectorAll(sel[i]);
if (list && list.length) return { parent: list[0], before: null };
}
return null;
}

function ipmBuild() {
var h, wrap, btn, dot, lab, car;
if (el('ipm-btn')) return;
h = ipmHost();
if (!h) return;
wrap = mk('div', 'ipm-wrap', ''); wrap.id = 'ipm-wrap';
btn = mk('button', 'ipm-btn', ''); btn.id = 'ipm-btn'; btn.type = 'button';
dot = mk('span', 'ipm-dot', ''); dot.id = 'ipm-dot';
lab = mk('span', 'ipm-lab', ''); lab.id = 'ipm-lab';
car = mk('span', 'ipm-car', '\u25BE');
btn.appendChild(dot); btn.appendChild(lab); btn.appendChild(car);
btn.onclick = ipmToggle;
wrap.appendChild(btn);
if (h.before) h.parent.insertBefore(wrap, h.before); else h.parent.appendChild(wrap);
window.addEventListener('resize', ipmPlace, false);
window.addEventListener('scroll', ipmPlace, true);
ipmSync();
}

function ipmSync() {
var btn = el('ipm-btn'), lab = el('ipm-lab'), dot = el('ipm-dot'),
cur, link, label, cls, st, sd, note, s = {};
if (!btn || !lab) return;
try { s = S.status || {}; } catch (e) {}
cur = ipmCurrent();
link = ipmLink();
label = ipmStateText(link);
cls = link ? ' ' + link : '';
btn.className = 'ipm-btn' + ((!cur || link === 'bad') ? ' bad' : cls);
btn.title = ipmT('title') + ' \u2014 ' + label;
lab.textContent = cur ? cur : ipmT('set');
if (dot) dot.className = 'ipm-dot' + ((!cur || link === 'bad') ? ' bad' : cls);
st = el('ipm-state-txt'); sd = el('ipm-state-dot');
if (st) { st.textContent = label; st.className = 'ipm-state-txt' + cls; }
if (sd) sd.className = 'ipm-dot' + cls;
note = el('ipm-note');
if (note) {
note.innerHTML = '';
if (link === 'bad' || !cur) {
note.className = 'ipm-note show';
note.appendChild(mk('b', '', ipmT('offline')));
note.appendChild(document.createTextNode(' \u2014 ' + ipmT('noteOff')));
if (s.err) note.appendChild(mk('div', 'ipm-err', String(s.err).substring(0, 150)));
} else if (link === 'warn') {
note.className = 'ipm-note show warn';
note.appendChild(document.createTextNode(ipmT('noteWarn')));
if (s.err) note.appendChild(mk('div', 'ipm-err', String(s.err).substring(0, 150)));
} else {
note.className = 'ipm-note';
}
}
}

function ipmPlace() {
var btn = el('ipm-btn'), pop = el('ipm-pop'), vw, vh, r, w, h, left, top;
if (!btn || !pop) return;
vw = document.documentElement.clientWidth;
vh = document.documentElement.clientHeight;
r = btn.getBoundingClientRect();
w = pop.offsetWidth; h = pop.offsetHeight;
left = r.right - w;
if (left + w > vw - 12) left = vw - w - 12;
if (left < 12) left = 12;
top = r.bottom + 8;
if (top + h > vh - 12) { top = r.top - h - 8; if (top < 12) top = 12; }
pop.style.left = left + 'px';
pop.style.top = top + 'px';
}

function ipmMsg(text, ok) {
var m = el('ipm-msg');
if (!m) return;
m.className = 'ipm-msg' + (ok ? ' ok' : '');
if (text) m.textContent = text; else m.innerHTML = '&nbsp;';
}

function ipmClose() {
var p = el('ipm-pop'), s = el('ipm-scrim');
if (p && p.parentNode) p.parentNode.removeChild(p);
if (s && s.parentNode) s.parentNode.removeChild(s);
}

function ipmToggle() { if (el('ipm-pop')) ipmClose(); else ipmOpen(); }

function ipmOpen() {
var btn = el('ipm-btn'), scrim, pop, head, field, inp, go, msg, note, foot, state, sd, stt, cancel;
if (!btn || el('ipm-pop')) return;

scrim = mk('div', 'ipm-scrim', ''); scrim.id = 'ipm-scrim';
scrim.onclick = ipmClose;
document.body.appendChild(scrim);

pop = mk('div', 'ipm-pop', ''); pop.id = 'ipm-pop';
head = mk('div', 'ipm-h', '');
head.appendChild(mk('i', '', ''));
head.appendChild(mk('span', '', ipmT('title')));
pop.appendChild(head);
pop.appendChild(mk('div', 'ipm-sub', ipmT('sub')));

field = mk('div', 'ipm-field', '');
inp = mk('input', 'ipm-inp', ''); inp.id = 'ipm-inp'; inp.type = 'text';
inp.setAttribute('placeholder', ipmT('ph'));
inp.setAttribute('autocomplete', 'off');
inp.setAttribute('spellcheck', 'false');
inp.value = ipmCurrent();
go = mk('button', 'ipm-go', ipmT('go')); go.id = 'ipm-go'; go.type = 'button';
go.onclick = ipmApply;
field.appendChild(inp); field.appendChild(go);
pop.appendChild(field);

msg = mk('div', 'ipm-msg', ''); msg.id = 'ipm-msg'; msg.innerHTML = '&nbsp;';
pop.appendChild(msg);

note = mk('div', 'ipm-note', ''); note.id = 'ipm-note';
pop.appendChild(note);

foot = mk('div', 'ipm-foot', '');
state = mk('div', 'ipm-state', '');
sd = mk('span', 'ipm-dot', ''); sd.id = 'ipm-state-dot';
stt = mk('span', 'ipm-state-txt', ''); stt.id = 'ipm-state-txt';
state.appendChild(sd); state.appendChild(stt);
cancel = mk('button', 'ipm-cancel', ipmT('cancel')); cancel.type = 'button';
cancel.onclick = ipmClose;
foot.appendChild(state); foot.appendChild(cancel);
pop.appendChild(foot);

inp.onkeydown = function(ev) {
var e = ev || window.event, k = e.keyCode || e.which;
if (k === 13) { ipmApply(); if (e.preventDefault) e.preventDefault(); return false; }
if (k === 27) { ipmClose(); if (e.preventDefault) e.preventDefault(); return false; }
};
inp.oninput = function() { this.className = 'ipm-inp'; ipmMsg('', false); };

document.body.appendChild(pop);
ipmSync();
ipmPlace();
try { inp.focus(); inp.select(); } catch (e) {}
}

function ipmApply() {
var inp = el('ipm-inp'), go = el('ipm-go'), v, f, port, path;
if (!inp) return;
v = ipmClean(inp.value);
if (!ipmValid(v)) {
inp.className = 'ipm-inp err';
ipmMsg(ipmT('bad'), false);
try { inp.focus(); inp.select(); } catch (e) {}
return;
}
f = el('c-ip');
if (f) f.value = v;
port = el('c-port') ? (parseInt(el('c-port').value, 10) || 9999) : 9999;
path = (el('c-path') && el('c-path').value) ? el('c-path').value : '/websocket';
send('config', { ip: v, ws_port: port, ws_path: path, http_fallback: !!S.fb });
ipmMsg(ipmT('sent') + v, true);
if (go) go.disabled = true;
try { toast(ipmT('sent') + v, 'info'); } catch (e) {}
window.setTimeout(function() {
try { if (typeof applyCamMode === 'function') applyCamMode(true); } catch (e) {}
}, 600);
window.setTimeout(ipmClose, 1200);
}

var LANG_TITLE = { en: 'Interface language', ru: 'Язык интерфейса', zh: '界面语言' };

/* Compact language picker in the top bar. Applies instantly, no reload. */
function langBuild() {
var anchor = el('btn-sound'), wrap, sel, opts, i, o;
if (el('lang-sel')) return;
if (!anchor || !anchor.parentNode) return;
wrap = mk('div', 'ipm-wrap', '');
sel = mk('select', 'inp', '');
sel.id = 'lang-sel';
sel.style.cssText = 'height:30px;padding:0 9px;font-size:11.5px;font-weight:700;' +
'border-radius:999px;width:auto;cursor:pointer;';
opts = [['en', 'English'], ['ru', 'Русский'], ['zh', '中文']];
for (i = 0; i < opts.length; i++) {
o = mk('option', null, opts[i][1]);
o.value = opts[i][0];
sel.appendChild(o);
}
sel.value = LANG;
sel.title = LANG_TITLE[LANG] || LANG_TITLE.en;
sel.onchange = function() {
var v = this.value;
send('ui_lang', { lang: v });
applyUiLang(v);
};
wrap.appendChild(sel);
anchor.parentNode.insertBefore(wrap, anchor);
}

function applyUiLang(v) {
var s;
if (v !== 'en' && v !== 'ru' && v !== 'zh') return;
if (v === LANG) return;
LANG = v;
S.calibSig = ''; S.capsRendered = ''; S.limSig = ''; S.flowSig = '';
S.posSig = ''; S.heatSig = ''; S.badgeKey = '';
applyLang();
renderFiles(); renderHistory(); renderCamSources(); renderCamStats();
renderMotionState();
s = el('lang-sel');
if (s) { s.value = v; s.title = LANG_TITLE[v] || LANG_TITLE.en; }
}

function renderMediaState() {
var m = S.media || {};
var hb = el('h-media');
if (hb && hb.parentNode && hb.parentNode.style.display !== 'none') {
hb.parentNode.style.display = 'none';
}
el('h-media-state').textContent = '';
el('h-media').disabled = !!m.busy;
el('c-media').disabled = !!m.busy;
ipmBuild();
ipmSync();
langBuild();
}

function renderHistory() {
  el('hv-jobs').textContent = String(S.hstats.jobs || S.history.length);
  el('hv-time').textContent = fmtTime(S.hstats.time || 0);
  el('hv-fil').textContent = fmtFil(S.hstats.filament || 0);
  el('cnt-hist').textContent = S.history.length ? String(S.history.length) : '';
  renderMediaState();
  var list = el('hlist'); list.innerHTML = '';
  if (!S.history.length) { list.appendChild(mk('div', 'empty', t('no_history'))); renderHDetail(); return; }
  var i;
  for (i = 0; i < S.history.length; i++) {
    (function(h) {
      var row = mk('div', 'hrow' + (S.hsel === h.id ? ' sel' : ''));
      var b = mk('div', 'hbadge ' + (h.finish ? 'ok' : 'bad'));
      b.appendChild(svgIcon(h.finish ? IC_CHECK : IC_X));
      row.appendChild(b);
      var main = mk('div', 'hmain');
      main.appendChild(mk('div', 'hname', h.name));
      main.appendChild(mk('div', 'hsub', fmtDate(h.start) + '  ·  ' + fmtTime(h.total) + '  ·  ' + fmtFil(h.filament)));
      row.appendChild(main);
      if (h.material) row.appendChild(mk('span', 'mtag', h.material));
      if (h.video_path) row.appendChild(mk('span', 'tl', t('timelapse')));
      row.onclick = function() { S.hsel = h.id; renderHistory(); };
      list.appendChild(row);
    })(S.history[i]);
  }
  renderHDetail();
}

function renderHDetail() {
  var box = el('hdetail'); box.innerHTML = '';
  var h = null, i;
  for (i = 0; i < S.history.length; i++) { if (S.history[i].id === S.hsel) h = S.history[i]; }
  if (!h) { box.appendChild(mk('div', 'empty', t('pick_file'))); return; }
  if (h.video_url) {
    var v = document.createElement('video');
    v.controls = true; v.preload = 'metadata'; v.src = h.video_url;
    if (h.cover_url) v.poster = h.cover_url;
    v.onerror = function() {
      var code = (v.error && v.error.code) ? v.error.code : 0;
      var msg = (v.error && v.error.message) ? v.error.message : '';
      jlog('TIMELAPSE error code=' + code + ' ' + msg + ' src=' + h.video_url);
      if (v.parentNode) v.parentNode.replaceChild(mk('div', 'warnbox', t('video_fail') + ' [code ' + code + '] ' + msg), v);
    };
    box.appendChild(v);
} else if (h.video_path) {
box.appendChild(mk('div', 'warnbox', t('video_fail')));
}
  box.appendChild(drow(t('material'), h.material || t('na')));
  box.appendChild(drow(h.finish ? t('finished') : t('failed'), fmtDate(h.start)));
  box.appendChild(drow(t('total_time'), fmtTime(h.total)));
  box.appendChild(drow(t('elapsed'), fmtTime(h.used)));
  box.appendChild(drow(t('filament'), fmtFil(h.filament)));
  box.appendChild(drow(t('size'), fmtSize(h.size)));
  if (h.video_path) box.appendChild(drow(t('timelapse'), fmtTime(h.video_duration) + ' · ' + fmtSize(h.video_size)));
  box.appendChild(drow(t('path'), h.path));
  var act = mk('div', 'dactions');
  var bd = mk('button', 'btn danger'); bd.appendChild(svgIcon(IC_TRASH));
  bd.appendChild(document.createTextNode(' ' + t('remove_entry')));
  bd.onclick = function() {
    send('history_delete', {id: h.id, name: h.name});
    toast(h.name + ' — ' + t('deleted'), 'info', function() { send('history_undo', {id: h.id}); }, 6000);
  };
  act.appendChild(bd); box.appendChild(act);
}

/* ------------------------------------------------------------------ WebRTC */
function rtcEnvDump() {
  var v = el('cam-video');
  jlog('ENV ua=' + navigator.userAgent);
  jlog('ENV origin=' + window.location.href);
  jlog('ENV RTCPeerConnection=' + (window.RTCPeerConnection ? 'yes' : 'no'));
  var probe = document.createElement('video');
  jlog('ENV canPlayType mp4="' + probe.canPlayType('video/mp4') +
       '" h264="' + probe.canPlayType('video/mp4; codecs="avc1.42E01E"') +
       '" webm="' + probe.canPlayType('video/webm; codecs="vp8"') + '"');
  if (window.RTCRtpReceiver && RTCRtpReceiver.getCapabilities) {
    var caps = RTCRtpReceiver.getCapabilities('video');
    if (caps && caps.codecs) {
      var names = [], i;
      for (i = 0; i < caps.codecs.length; i++) names.push(caps.codecs[i].mimeType);
      jlog('ENV recv codecs=' + names.join(', '));
    } else { jlog('ENV recv codecs=none'); }
  } else { jlog('ENV getCapabilities unavailable'); }
}

function rtcStop() {
  if (S.rtcWatch) { clearInterval(S.rtcWatch); S.rtcWatch = null; }
  if (S.pc) {
    jlog('RTC closing pc');
    try { S.pc.close(); } catch (e) {}
    S.pc = null;
  }
  var v = el('cam-video');
  try { v.srcObject = null; } catch (e2) { v.removeAttribute('src'); }
  S.rtcWaiting = false; S.rtcSent = false; S.rtcState = ''; S.rtcStats = null;
}

function rtcAlive() {
  if (!S.pc) return false;
  var st = S.pc.iceConnectionState;
  return st === 'new' || st === 'checking' || st === 'connected' || st === 'completed';
}

function rtcSendOffer() {
  if (S.rtcSent || !S.pc || !S.pc.localDescription) return;
  S.rtcSent = true;
  var payload;
  try { payload = btoa(JSON.stringify({type: 'offer', sdp: S.pc.localDescription.sdp})); }
  catch (e) { S.cam.err = String(e); jlog('RTC btoa failed ' + e); camMessage('error'); return; }
  jlog('RTC sending offer, gathering=' + S.pc.iceGatheringState);
  send('rtc_offer', {body: payload, seq: S.rtcSeq});
}

function rtcStatsTick() {
  if (!S.pc || !S.pc.getStats) return;
  S.pc.getStats().then(function(rep) {
    var found = null, codec = '';
    rep.forEach(function(r) {
      if (r.type === 'inbound-rtp' && (r.kind === 'video' || r.mediaType === 'video')) {
        found = {bytes: r.bytesReceived || 0, recv: r.framesReceived || 0,
                 dec: r.framesDecoded || 0, key: r.keyFramesDecoded || 0,
                 w: r.frameWidth || 0, h: r.frameHeight || 0, codecId: r.codecId};
      }
    });
    if (found && found.codecId) {
      rep.forEach(function(r) { if (r.id === found.codecId && r.mimeType) codec = r.mimeType; });
    }
    if (found) {
      S.rtcStats = found;
      if (codec) S.rtcCodec = codec;
      if (found.w && !S.camW) { S.camW = found.w; S.camH = found.h; camMessage('hide'); }
      if (now() - S.rtcLastStatsLog > 4000) {
        S.rtcLastStatsLog = now();
        jlog('RTC stats bytes=' + found.bytes + ' recv=' + found.recv + ' dec=' + found.dec +
             ' key=' + found.key + ' size=' + found.w + 'x' + found.h + ' codec=' + (codec || S.rtcCodec));
      }
    }
    renderCamStats();
  })['catch'](function(e) { jlog('RTC getStats failed ' + e); });
}

function rtcGrabSnapshot() {
  var v = el('cam-video');
  if (!v || v.videoWidth === 0) {
    toast(t('cam_error'), 'err');
    return;
  }
  try {
    var canvas = document.createElement('canvas');
    canvas.width = v.videoWidth;
    canvas.height = v.videoHeight;
    var ctx = canvas.getContext('2d');
    ctx.drawImage(v, 0, 0);
    var dataUrl = canvas.toDataURL('image/jpeg', 0.92);
    send('cam_snap_webrtc', {jpeg: dataUrl});
    toast(t('cam_saving'), 'info');
  } catch (e) {
    jlog('RTC snapshot failed: ' + e);
    toast(t('cam_error'), 'err');
  }
}

function rtcStart() {
  rtcStop();
  var img = el('cam-img'), v = el('cam-video');
  img.style.display = 'none'; img.src = '';
  v.style.display = 'block';
  S.camEff = 'webrtc'; S.camW = 0; S.camH = 0; S.rtcStart = now();
  send('cam_stop', {});
  if (!S.rtcEnvLogged) { S.rtcEnvLogged = true; rtcEnvDump(); }
  if (!window.RTCPeerConnection) { camMessage('rtc_unsupported'); jlog('RTC no RTCPeerConnection'); return; }
  camMessage('connecting');

  var pc;
  try {
    pc = new RTCPeerConnection({
      iceServers: [{urls: 'stun:stun.l.google.com:19302'}]
    });
  } catch (e) {
    S.cam.err = String(e); jlog('RTC ctor failed ' + e); camMessage('error'); return;
  }
  S.pc = pc;
  S.rtcSeq = S.rtcSeq + 1;
  S.rtcWaiting = true; S.rtcSent = false; S.rtcLastStatsLog = 0;
  jlog('RTC start seq=' + S.rtcSeq + ' (creality-ref mode, STUN=google)');

  function sendOfferToCall(sdp) {
    S.rtcSent = true;
    var payload;
    try { payload = btoa(JSON.stringify({type: 'offer', sdp: sdp})); }
    catch (e) { S.cam.err = String(e); jlog('RTC btoa failed ' + e); camMessage('error'); return; }
    jlog('RTC sending offer, gathering=' + pc.iceGatheringState);
    send('rtc_offer', {body: payload, seq: S.rtcSeq});
  }

  pc.ontrack = function(ev) {
    jlog('RTC ontrack kind=' + ev.track.kind + ' state=' + ev.track.readyState +
      ' streams=' + (ev.streams ? ev.streams.length : 0));
    if (ev.streams && ev.streams[0]) {
      v.srcObject = ev.streams[0];
      v.autoplay = true;
      v.controls = false;
      v.muted = true;
      var p = v.play();
      if (p && p['catch']) p['catch'](function(err) { jlog('RTC play rejected ' + err); });
    }
  };

  pc.oniceconnectionstatechange = function() {
    S.rtcState = pc.iceConnectionState;
    jlog('RTC iceConnectionState=' + pc.iceConnectionState);
    if (pc.iceConnectionState === 'failed') camMessage('rtc_failed');
    if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
      if (S.camW > 0) camMessage('hide');
    }
    renderCamStats();
  };

  pc.onconnectionstatechange = function() {
    jlog('RTC connectionState=' + pc.connectionState);
    if (pc.connectionState === 'connected') camMessage('hide');
  };

  pc.onicegatheringstatechange = function() {
    jlog('RTC gathering=' + pc.iceGatheringState);
  };

  pc.onicecandidate = function(ev) {
    if (ev.candidate === null) {
      jlog('RTC gathering complete, sending offer');
      sendOfferToCall(pc.localDescription.sdp);
    }
  };

  try {
    pc.addTransceiver('video', {direction: 'sendrecv'});
  } catch (e3) {
    jlog('RTC addTransceiver failed ' + e3);
  }

  pc.createOffer().then(function(d) {
    return pc.setLocalDescription(d);
  })['catch'](function(e) {
    S.cam.err = String(e); jlog('RTC createOffer failed ' + e); camMessage('error');
  });

  S.rtcWatch = setInterval(rtcStatsTick, 2000);

  setTimeout(function() {
    if (S.pc === pc && S.camW === 0) {
      jlog('RTC no decoded video 15s after start');
      camMessage('rtc_noframes');
    }
  }, 15000);
}

function rtcHandle(rtc) {
  if (!S.rtcWaiting || !rtc || rtc.seq !== S.rtcSeq) return;
  if (rtc.answer) {
    S.rtcWaiting = false;
    try {
      var obj = JSON.parse(rtc.answer);
      jlog('RTC applying answer type=' + obj.type);
      S.pc.setRemoteDescription(new RTCSessionDescription(obj))['catch'](function(e) {
        S.cam.err = String(e); jlog('RTC setRemoteDescription failed ' + e); camMessage('error');
      });
    } catch (e) { S.cam.err = 'bad answer'; jlog('RTC answer parse failed ' + e); camMessage('error'); }
  } else if (rtc.err) {
    S.rtcWaiting = false;
    S.cam.err = rtc.err;
    camMessage('error');
  }
}

/* ------------------------------------------------------------------ camera */
function camCfg(patch) {
  var k;
  for (k in patch) { if (patch.hasOwnProperty(k)) S.cs[k] = patch[k]; }
  send('cam_cfg', {patch: patch});
}
function renderCamControls() {
  var cs = S.cs;
  el('cam-mode').value = cs.cam_mode;
  el('rtc-port').value = cs.rtc_port;
  el('rtc-h264').className = cs.rtc_h264_first ? 'toggle on' : 'toggle';
  el('cam-fps').value = cs.cam_fps; el('v-cam-fps').textContent = cs.cam_fps;
  el('cam-zoom').value = cs.cam_zoom; el('v-cam-zoom').textContent = cs.cam_zoom + '%';
  el('cam-rot').value = String(cs.cam_rot);
  el('cam-bright').value = cs.cam_bright; el('v-cam-bright').textContent = cs.cam_bright + '%';
  el('cam-contrast').value = cs.cam_contrast; el('v-cam-contrast').textContent = cs.cam_contrast + '%';
  el('cam-sat').value = cs.cam_sat; el('v-cam-sat').textContent = cs.cam_sat + '%';
  el('cam-fliph').className = cs.cam_fliph ? 'toggle on' : 'toggle';
  el('cam-flipv').className = cs.cam_flipv ? 'toggle on' : 'toggle';
  el('cam-grid-tg').className = cs.cam_grid ? 'toggle on' : 'toggle';
  el('cam-hud-tg').className = cs.cam_hud ? 'toggle on' : 'toggle';
  el('cam-grid').className = cs.cam_grid ? 'cam-grid on' : 'cam-grid';
  el('cam-hud').className = cs.cam_hud ? 'cam-hud on' : 'cam-hud';
  el('cam-power').textContent = t('cam_power') + ': ' + (S.camOn ? t('on') : t('off'));
  el('cam-power').className = S.camOn ? 'btn cool sm wide' : 'btn ghost sm wide';
  applyCamStyle();
}
function applyCamStyle() {
  var cs = S.cs;
  var tr = 'rotate(' + cs.cam_rot + 'deg) scale(' + (cs.cam_zoom / 100) + ')';
  if (cs.cam_fliph) tr += ' scaleX(-1)';
  if (cs.cam_flipv) tr += ' scaleY(-1)';
  var flt = 'brightness(' + cs.cam_bright + '%) contrast(' + cs.cam_contrast + '%) saturate(' + cs.cam_sat + '%)';
  var nodes = [el('cam-img'), el('cam-video')], i;
  for (i = 0; i < nodes.length; i++) { nodes[i].style.transform = tr; nodes[i].style.filter = flt; }
}
function renderCamSources() {
  var sel = el('cam-src'), list = S.cam.sources || [], i;
  sel.innerHTML = '';
  if (!list.length) { var o = mk('option', null, t('cam_nosource')); o.value = ''; sel.appendChild(o); }
  for (i = 0; i < list.length; i++) {
    var op = mk('option', null, list[i].label); op.value = list[i].url; sel.appendChild(op);
  }
  var custom = mk('option', null, t('cam_custom')); custom.value = '__custom__'; sel.appendChild(custom);
  var match = false;
  for (i = 0; i < list.length; i++) { if (list[i].url === S.cam.url) match = true; }
  sel.value = match ? S.cam.url : '__custom__';
  el('cam-url').value = S.cam.url || '';
  el('cam-detect').textContent = S.cam.probing ? t('cam_detecting') : t('cam_detect');
  el('cam-detect').disabled = !!S.cam.probing;
}
function camMessage(kind) {
  var box = el('cam-msg');
  if (kind === 'hide') { box.className = 'cam-msg hide'; return; }
  box.className = 'cam-msg';
  var ttl = '', sub = '', btn = '';
  if (kind === 'off') { ttl = t('cam_idle'); sub = t('cam_direct_hint'); btn = t('cam_power'); }
  else if (kind === 'connecting') { ttl = t('cam_connecting'); sub = S.camEff === 'webrtc' ? ('WebRTC · port ' + S.cs.rtc_port) : (S.cam.url || ''); }
  else if (kind === 'nosource') { ttl = t('cam_nosource'); sub = t('cam_direct_hint'); btn = t('cam_detect'); }
  else if (kind === 'rtc_unsupported') { ttl = t('rtc_unsupported'); sub = t('cam_direct_hint'); }
  else if (kind === 'rtc_failed') { ttl = t('rtc_failed'); sub = S.cam.err || ''; }
  else if (kind === 'rtc_noframes') {
    ttl = t('rtc_noframes');
    var st = S.rtcStats;
    sub = st ? ('bytes ' + st.bytes + ' · recv ' + st.recv + ' · dec ' + st.dec + ' · ' + (S.rtcCodec || '?')) : (S.cam.err || '');
  }
  else if (kind === 'error') { ttl = t('cam_error'); sub = S.cam.err || ''; btn = t('cam_detect'); }
  el('cam-msg-ttl').textContent = ttl;
  el('cam-msg-sub').textContent = sub;
  var b = el('cam-msg-btn');
  if (btn) { b.style.display = 'inline-flex'; b.textContent = btn; b.setAttribute('data-act', kind === 'off' ? 'power' : 'detect'); }
  else b.style.display = 'none';
}
function renderCamStats() {
  var c = S.cam, label;
  if (S.camEff === 'webrtc') {
    label = S.camW > 0 ? t('cam_live') : (S.rtcState || t('cam_connecting'));
  } else {
    label = c.state === 'live' ? t('cam_live') : (c.state === 'connecting' ? t('cam_connecting')
      : (c.state === 'error' ? t('cam_error') : t('cam_idle')));
    if (S.camEff === 'direct' && S.camW > 0) label = t('cam_live');
  }
  el('cv-state').textContent = S.viewSnap ? t('cam_view_btn') : label;
  el('cv-res').textContent = S.camW ? (S.camW + '×' + S.camH) : t('na');
  el('cv-fps').textContent = S.camEff === 'webrtc' ? (S.rtcState || t('na')) : (c.fps ? c.fps.toFixed(1) : t('na'));
  if (S.camEff === 'webrtc') {
    var st = S.rtcStats;
    el('cv-rate').textContent = st ? (st.dec + ' / ' + st.recv) : t('na');
  } else {
    el('cv-rate').textContent = c.kbps ? Math.round(c.kbps) + ' KB/s' : t('na');
  }
  var up = S.camEff === 'webrtc' ? (S.rtcStart ? Math.floor((now() - S.rtcStart) / 1000) : 0) : c.uptime;
  el('cv-up').textContent = up ? fmtClock(up) : t('na');
  el('hud-host').textContent = (S.status.hostname || 'K1C') + ' · ' + (S.status.model || 'K1C');
  el('hud-state').textContent = S.viewSnap ? 'STILL' : (S.camW > 0 ? 'LIVE' : 'IDLE');
  var extra = S.camEff === 'webrtc' ? (S.rtcCodec || 'WEBRTC') : (S.camEff === 'direct' ? 'DIRECT' : 'RELAY');
  el('hud-tech').textContent = (S.camW ? (S.camW + '×' + S.camH) : '--') + '  ·  ' + extra + '  ·  ' + S.cs.cam_zoom + '%';
  var d = new Date();
  function p(n) { return n < 10 ? '0' + n : '' + n; }
  el('hud-time').textContent = p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
}
function renderSnaps() {
  var list = S.cam.snaps || [], box = el('snaplist'), i;
  box.innerHTML = '';
  el('snap-count').textContent = String(list.length);
  if (!list.length) { box.appendChild(mk('div', 'empty', t('cam_nosnaps'))); return; }
  for (i = 0; i < list.length; i++) {
    (function(sn) {
      var row = mk('div', 'snaprow');
      var ic = mk('div', 'snapico'); ic.appendChild(svgIcon(IC_SHOT)); row.appendChild(ic);
      var mid = mk('div', 'nm');
      mid.appendChild(document.createTextNode(sn.name));
      mid.appendChild(mk('div', 'mt', fmtDate(sn.ts) + '  ·  ' + fmtSize(sn.size)));
      row.appendChild(mid);
      var bv = mk('button', 'btn ghost sm', t('cam_view_btn'));
      bv.onclick = function() { send('cam_snap_view', {name: sn.name}); };
      var bd = mk('button', 'iconbtn d'); bd.appendChild(svgIcon(IC_TRASH));
      bd.title = t('delete');
      bd.onclick = function() { send('cam_snap_del', {name: sn.name}); };
      row.appendChild(bv); row.appendChild(bd);
      box.appendChild(row);
    })(list[i]);
  }
}
function clearDirectTimer() { if (S.directTimer) { clearTimeout(S.directTimer); S.directTimer = null; } }
function startDirect() {
  var img = el('cam-img');
  el('cam-video').style.display = 'none';
  img.style.display = 'block';
  S.camEff = 'direct'; S.camW = 0; S.camH = 0;
  send('cam_stop', {});
  camMessage('connecting');
  clearDirectTimer();
  img.src = ''; img.src = S.cam.url;
  S.directTimer = setTimeout(function() {
    if (S.tab !== 'camera' || S.viewSnap) return;
    if (img.naturalWidth === 0) {
      if (S.cs.cam_mode === 'auto') { S.camEff = 'relay'; img.src = ''; camMessage('connecting'); send('cam_start', {}); }
      else camMessage('error');
    }
  }, 4500);
}
function applyCamMode(force) {
  var img = el('cam-img');
  if (S.viewSnap) return;
  if (!S.camOn) {
    clearDirectTimer(); rtcStop(); send('cam_stop', {});
    img.src = ''; img.style.display = 'block'; el('cam-video').style.display = 'none';
    S.camEff = 'off'; camMessage('off'); renderCamStats(); return;
  }
  var mode = S.cs.cam_mode;
  if (mode === 'webrtc' || (mode === 'auto' && window.RTCPeerConnection)) {
    if (!force && S.camEff === 'webrtc' && rtcAlive()) {
      el('cam-video').style.display = 'block';
      img.style.display = 'none';
      renderCamStats();
      return;
    }
    rtcStart(); renderCamStats(); return;
  }
  rtcStop();
  el('cam-video').style.display = 'none';
  img.style.display = 'block';
  if (!S.cam.url) {
    clearDirectTimer(); img.src = ''; S.camEff = 'off';
    camMessage('nosource'); send('cam_start', {}); renderCamStats(); return;
  }
  if (mode === 'direct') { startDirect(); return; }
  clearDirectTimer(); S.camEff = 'relay'; img.src = ''; camMessage('connecting'); send('cam_start', {});
}
function camTick() {
  if (S.tab !== 'camera' || !S.camOn || S.viewSnap || S.camEff !== 'relay') return;
  send('cam_frame', {seq: S.cam.seq});
}
function bindCamSlider(id, key, labelId, suffix, restart) {
  var e = el(id);
  e.oninput = function() {
    S.cs[key] = parseInt(this.value, 10);
    if (labelId) el(labelId).textContent = S.cs[key] + (suffix || '');
    applyCamStyle(); renderCamStats();
  };
  e.onchange = function() {
    var patch = {}; patch[key] = S.cs[key];
    camCfg(patch);
    if (restart && S.camEff === 'relay') send('cam_start', {});
  };
}

function bindFan(k, kind) {
  var s = el('s-' + k);
  s.oninput = function() {
    S.touchF[k] = now();
    var v = parseInt(this.value, 10);
    el('v-' + k).textContent = v + '%';
    el('tg-' + k).textContent = v > 0 ? t('on') : t('off');
    el('tg-' + k).className = v > 0 ? 'toggle on' : 'toggle';
  };
  s.onchange = function() { S.touchF[k] = now(); send('control', {kind: kind, value: parseInt(this.value, 10)}); };
  el('tg-' + k).onclick = function() {
    var v = parseInt(s.value, 10) > 0 ? 0 : 100;
    s.value = v; S.touchF[k] = now();
    el('v-' + k).textContent = v + '%';
    this.textContent = v > 0 ? t('on') : t('off');
    this.className = v > 0 ? 'toggle on' : 'toggle';
    send('control', {kind: kind, value: v});
  };
}
function bindMode(key) {
  el('mm-' + key).onclick = function() { send('control', {kind: 'feedrate', value: modeValues()[key]}); };
}
function toggleLock() { S.jog.lock = !S.jog.lock; renderJogControls(); }

function showSub(which) {
  S.sub = which;
  el('ctl-main').style.display = which === 'main' ? 'block' : 'none';
  el('ctl-calib').style.display = which === 'calib' ? 'block' : 'none';
  el('sub-main').className = which === 'main' ? 'sub on' : 'sub';
  el('sub-calib').className = which === 'calib' ? 'sub on' : 'sub';
  if (which === 'calib') { send('caps_refresh', {force: false}); renderCalib(true); renderMacros(true); }
  S.posSig = '';
  renderMotionState();
}

function showTab(which) {
  S.tab = which;
  el('view-control').style.display = which === 'control' ? 'block' : 'none';
  el('view-files').style.display = which === 'files' ? 'block' : 'none';
  el('view-history').style.display = which === 'history' ? 'block' : 'none';
  el('view-camera').style.display = which === 'camera' ? 'block' : 'none';
  el('subbar').style.display = which === 'control' ? 'flex' : 'none';
  el('tab-control').className = which === 'control' ? 'tab on' : 'tab';
  el('tab-files').className = which === 'files' ? 'tab on' : 'tab';
  el('tab-history').className = which === 'history' ? 'tab on' : 'tab';
  el('tab-camera').className = which === 'camera' ? 'tab on' : 'tab';
  if (which === 'files') renderFiles();
  if (which === 'history') { send('history_get', {}); renderHistory(); }
  if (which === 'camera') { renderCamSources(); renderSnaps(); renderCamStats(); applyCamMode(false); }
  if (which === 'control') showSub(S.sub);
}

function appendLog(lines) {
  if (!lines || !lines.length) return;
  var box = el('dlog'), i;
  for (i = 0; i < lines.length; i++) box.appendChild(document.createTextNode(lines[i] + '\\n'));
  while (box.childNodes.length > 600) box.removeChild(box.firstChild);
  box.scrollTop = box.scrollHeight;
}

function bind() {
  el('tab-control').onclick = function() { showTab('control'); };
  el('tab-files').onclick = function() { showTab('files'); };
  el('tab-history').onclick = function() { showTab('history'); };
  el('tab-camera').onclick = function() { showTab('camera'); };
  el('sub-main').onclick = function() { showSub('main'); };
  el('sub-calib').onclick = function() { showSub('calib'); };
  el('btn-diag').onclick = function() {
    S.diagOpen = !S.diagOpen;
    el('diag').className = S.diagOpen ? 'card diag show' : 'card diag';
  };
  el('btn-sound').onclick = function() {
    S.cs.sound = !S.cs.sound;
    this.textContent = S.cs.sound ? t('sound_on') : t('sound_off');
    camCfg({sound: S.cs.sound});
    if (S.cs.sound) shutter();
  };
  el('scrim').onclick = function(ev) { if (ev.target === el('scrim')) closeModal(); };

  var ni = el('in-nozzle'), bi = el('in-bed');
  ni.onfocus = function() { S.touchT.nozzle = now(); };
  ni.oninput = function() { S.touchT.nozzle = now(); };
  bi.onfocus = function() { S.touchT.bed = now(); };
  bi.oninput = function() { S.touchT.bed = now(); };
  el('set-nozzle').onclick = function() {
    if (isBusy()) { toast(t('busy'), 'err'); return; }
    S.touchT.nozzle = now();
    var v = parseInt(ni.value, 10);
    if (isNaN(v)) return;
    if (v >= 150) S.cs.last_heat = v;
    send('control', {kind: 'nozzle', value: v});
  };
  el('set-bed').onclick = function() {
    if (isBusy()) { toast(t('busy'), 'err'); return; }
    S.touchT.bed = now();
    var v = parseInt(bi.value, 10);
    if (isNaN(v)) return;
    send('control', {kind: 'bed', value: v});
  };
  bindFan('model', 'model_fan'); bindFan('case', 'case_fan'); bindFan('aux', 'aux_fan');
  bindMode('silent'); bindMode('stable'); bindMode('standard'); bindMode('hyper');
  el('silent-val').onchange = function() {
    var v = parseInt(this.value, 10);
    if (isNaN(v)) { this.value = S.silent; return; }
    v = Math.max(10, Math.min(100, v));
    this.value = v; S.silent = v;
    send('motion_cfg', {value: v}); renderMotion();
  };
  el('tg-light').onclick = function() {
    var v = S.light ? 0 : 1;
    S.light = v;
    this.textContent = v ? t('on') : t('off');
    this.className = v ? 'toggle pri on' : 'toggle pri';
    send('control', {kind: 'light', value: v});
  };

  el('lock-sq-1').onclick = toggleLock;
  el('lock-sq-2').onclick = toggleLock;
  var pads = el('jogpad').getElementsByTagName('button'), i;
  for (i = 0; i < pads.length; i++) {
    (function(btn) {
      if (btn.id === 'jog-home-xy') { btn.onclick = function() { send('home', {axes: 'X Y'}); }; return; }
      btn.onclick = function() {
        var dx = parseInt(btn.getAttribute('data-dx'), 10);
        var dy = parseInt(btn.getAttribute('data-dy'), 10);
        if (dx) send('jog', {axis: 'x', dist: dx * S.jog.step, speed: S.jog.speed});
        if (dy) send('jog', {axis: 'y', dist: dy * S.jog.step, speed: S.jog.speed});
      };
    })(pads[i]);
  }
  el('z-up').onclick = function() { send('jog', {axis: 'z', dist: -S.jog.zstep, speed: S.jog.zspeed}); };
  el('z-down').onclick = function() { send('jog', {axis: 'z', dist: S.jog.zstep, speed: S.jog.zspeed}); };
  el('home-all').onclick = function() { send('home', {axes: ''}); };
  el('home-xy').onclick = function() { send('home', {axes: 'X Y'}); };
  el('home-z').onclick = function() { send('home', {axes: 'Z'}); };
  el('park-z').onclick = function() { send('park', {what: 'z'}); };
  el('center-xy').onclick = function() { send('park', {what: 'center'}); };
  el('motors-off').onclick = function() {
    confirmSimple(t('motors_title'), t('motors_body'), t('motors_off'), function() { send('motors_off', {}); });
  };
  var babies = document.querySelectorAll('[data-baby]');
  for (i = 0; i < babies.length; i++) {
    (function(b) { b.onclick = function() { send('z_offset', {delta: parseFloat(b.getAttribute('data-baby'))}); }; })(babies[i]);
  }
  el('zoff-reset').onclick = function() { send('z_offset', {reset: 1}); };

  el('btn-extrude').onclick = function() { send('extrude', {length: S.jog.len, speed: S.jog.fspeed}); };
  el('btn-retract').onclick = function() { send('extrude', {length: -S.jog.len, speed: S.jog.fspeed}); };
  el('btn-load').onclick = function() { send('filament', {kind: 'load'}); };
  el('btn-unload').onclick = function() { send('filament', {kind: 'unload'}); };
  el('btn-heat-now').onclick = function() {
    if (isBusy()) return;
    S.touchT.nozzle = now();
    send('control', {kind: 'nozzle', value: S.cs.last_heat});
  };
  el('btn-cooldown').onclick = function() {
    if (isBusy()) { toast(t('busy'), 'err'); return; }
    send('control', {kind: 'nozzle', value: 0});
    send('control', {kind: 'bed', value: 0});
  };
  var fs = el('s-flow');
  fs.oninput = function() { S.touchFlow = now(); el('v-flow').textContent = this.value + '%'; };
  fs.onchange = function() { S.touchFlow = now(); send('flow', {pct: parseInt(this.value, 10)}); };
  el('btn-save-config').onclick = function() {
    confirmSimple(t('save_title'), t('save_body'), t('save_config'), function() { send('calib', {kind: 'save_config'}); });
  };
  el('macro-q').oninput = function() { S.macroQ = this.value; renderMacros(true); };
  el('caps-refresh').onclick = function() { send('caps_refresh', {force: true}); };

  el('f-search').oninput = function() { S.q = this.value; renderFiles(); };
  el('f-sort').onchange = function() { S.sort = this.value; renderFiles(); };
  el('f-view').onclick = function() {
    S.view = S.view === 'grid' ? 'list' : 'grid';
    this.textContent = S.view === 'grid' ? t('grid') : t('list');
    renderFiles();
  };
  el('f-refresh').onclick = function() { send('files_refresh', {}); toast(t('sending'), 'info'); };
  el('bulk-all').onclick = function() {
    var l = visibleFiles(), k;
    for (k = 0; k < l.length; k++) S.checked[l[k].name] = 1;
    renderFiles();
  };
  el('bulk-clear').onclick = function() { S.checked = {}; renderFiles(); };
  el('bulk-del').onclick = function() {
    var arr = [], k;
    for (k in S.checked) {
      if (S.checked.hasOwnProperty(k) && S.checked[k]) { var f = findFile(k); if (f) arr.push(f); }
    }
    if (arr.length) deleteFiles(arr);
  };
  var sc = el('fgrid').parentNode, lastReq = 0;
  sc.onscroll = function() {
    if (now() - lastReq < 350) return;
    lastReq = now();
    send('poll', {cursor: S.cursor, thumbs: requestThumbs(), motion: S.tab === 'control'});
  };

  el('h-media').onclick = function() { send('media_probe', {}); toast(t('media_finding'), 'info'); };
  el('c-media').onclick = function() { send('media_probe', {}); toast(t('media_finding'), 'info'); };
  el('c-urltest').onclick = function() {
    var u = el('c-url').value;
    if (u) { send('media_test', {url: u}); toast(t('sending'), 'info'); }
  };

  el('cam-img').onload = function() {
    if (this.naturalWidth > 0) {
      S.camW = this.naturalWidth; S.camH = this.naturalHeight;
      if (!S.viewSnap) camMessage('hide');
      clearDirectTimer(); renderCamStats();
    }
  };
  el('cam-img').onerror = function() {
    if (S.viewSnap) return;
    if (S.camEff === 'direct' && S.cs.cam_mode === 'auto') {
      S.camEff = 'relay'; camMessage('connecting'); send('cam_start', {});
    }
  };
  var vid = el('cam-video');
  var vEvents = ['loadedmetadata', 'loadeddata', 'canplay', 'playing', 'play', 'waiting',
                 'stalled', 'suspend', 'emptied', 'error', 'resize'];
  for (i = 0; i < vEvents.length; i++) {
    (function(name) {
      vid.addEventListener(name, function() {
        var err = vid.error ? (' err=' + vid.error.code + ' ' + (vid.error.message || '')) : '';
        jlog('VIDEO ' + name + ' ' + vid.videoWidth + 'x' + vid.videoHeight +
             ' ready=' + vid.readyState + ' net=' + vid.networkState + err);
        if (name === 'loadedmetadata' || name === 'resize') {
          if (vid.videoWidth > 0) { S.camW = vid.videoWidth; S.camH = vid.videoHeight; camMessage('hide'); }
        }
        renderCamStats();
      });
    })(vEvents[i]);
  }
  vid.onclick = function() {
    var p = this.play();
    if (p && p['catch']) p['catch'](function(e) { jlog('VIDEO manual play rejected ' + e); });
  };

  el('cam-src').onchange = function() {
    if (this.value === '__custom__') { el('cam-url').focus(); return; }
    if (!this.value) return;
    var kind = 'mjpeg', k;
    for (k = 0; k < S.cam.sources.length; k++) {
      if (S.cam.sources[k].url === this.value) kind = S.cam.sources[k].kind;
    }
    el('cam-url').value = this.value; S.cam.url = this.value;
    camCfg({cam_url: this.value, cam_kind: kind});
    S.viewSnap = null; el('cam-back').style.display = 'none';
    applyCamMode(true);
  };
  el('cam-apply').onclick = function() {
    var u = el('cam-url').value.replace(/^\\s+|\\s+$/g, '');
    if (!u) { toast(t('cam_nosource'), 'err'); return; }
    var kind = u.indexOf('action=snapshot') >= 0 ? 'snapshot' : 'mjpeg';
    S.cam.url = u; camCfg({cam_url: u, cam_kind: kind});
    S.viewSnap = null; el('cam-back').style.display = 'none';
    applyCamMode(true); toast(t('sending'), 'info');
  };
  el('rtc-port').onchange = function() {
    var v = parseInt(this.value, 10);
    if (isNaN(v) || v < 1) { this.value = S.cs.rtc_port; return; }
    S.cs.rtc_port = v; camCfg({rtc_port: v});
    if (S.camEff === 'webrtc') applyCamMode(true);
  };
  el('rtc-h264').onclick = function() {
    S.cs.rtc_h264_first = !S.cs.rtc_h264_first;
    this.className = S.cs.rtc_h264_first ? 'toggle on' : 'toggle';
    camCfg({rtc_h264_first: S.cs.rtc_h264_first});
    if (S.camEff === 'webrtc') applyCamMode(true);
  };
  el('cam-restart').onclick = function() { applyCamMode(true); };
  el('rtc-diag').onclick = function() { rtcEnvDump(); rtcStatsTick(); toast(t('sending'), 'info'); };
  el('cam-detect').onclick = function() { send('cam_detect', {}); toast(t('cam_detecting'), 'info'); };
  el('cam-msg-btn').onclick = function() {
    if (this.getAttribute('data-act') === 'power') { S.camOn = true; renderCamControls(); applyCamMode(true); }
    else { send('cam_detect', {}); toast(t('cam_detecting'), 'info'); }
  };
  el('cam-mode').onchange = function() {
    S.cs.cam_mode = this.value; camCfg({cam_mode: this.value}); applyCamMode(true);
  };
  bindCamSlider('cam-fps', 'cam_fps', 'v-cam-fps', '', true);
  bindCamSlider('cam-zoom', 'cam_zoom', 'v-cam-zoom', '%', false);
  bindCamSlider('cam-bright', 'cam_bright', 'v-cam-bright', '%', false);
  bindCamSlider('cam-contrast', 'cam_contrast', 'v-cam-contrast', '%', false);
  bindCamSlider('cam-sat', 'cam_sat', 'v-cam-sat', '%', false);
  el('cam-rot').onchange = function() { S.cs.cam_rot = parseInt(this.value, 10); camCfg({cam_rot: S.cs.cam_rot}); applyCamStyle(); };
  el('cam-fliph').onclick = function() {
    S.cs.cam_fliph = !S.cs.cam_fliph;
    this.className = S.cs.cam_fliph ? 'toggle on' : 'toggle';
    camCfg({cam_fliph: S.cs.cam_fliph}); applyCamStyle();
  };
  el('cam-flipv').onclick = function() {
    S.cs.cam_flipv = !S.cs.cam_flipv;
    this.className = S.cs.cam_flipv ? 'toggle on' : 'toggle';
    camCfg({cam_flipv: S.cs.cam_flipv}); applyCamStyle();
  };
  el('cam-grid-tg').onclick = function() {
    S.cs.cam_grid = !S.cs.cam_grid;
    this.className = S.cs.cam_grid ? 'toggle on' : 'toggle';
    el('cam-grid').className = S.cs.cam_grid ? 'cam-grid on' : 'cam-grid';
    camCfg({cam_grid: S.cs.cam_grid});
  };
  el('cam-hud-tg').onclick = function() {
    S.cs.cam_hud = !S.cs.cam_hud;
    this.className = S.cs.cam_hud ? 'toggle on' : 'toggle';
    el('cam-hud').className = S.cs.cam_hud ? 'cam-hud on' : 'cam-hud';
    camCfg({cam_hud: S.cs.cam_hud});
  };
  el('cam-reset').onclick = function() {
    camCfg({cam_zoom: 100, cam_rot: 0, cam_fliph: false, cam_flipv: false,
            cam_bright: 100, cam_contrast: 100, cam_sat: 100});
    renderCamControls();
  };
  el('cam-power').onclick = function() {
    S.camOn = !S.camOn; S.viewSnap = null; el('cam-back').style.display = 'none';
    renderCamControls(); applyCamMode(true);
  };
  el('cam-snap').onclick = function() {
    if (S.camEff === 'webrtc') { rtcGrabSnapshot(); return; }
    send('cam_snap', {}); toast(t('cam_saving'), 'info');
  };
  el('cam-back').onclick = function() { S.viewSnap = null; this.style.display = 'none'; applyCamMode(true); };

  el('c-save').onclick = function() {
    send('config', {ip: el('c-ip').value, ws_port: parseInt(el('c-port').value, 10),
                    ws_path: el('c-path').value, http_fallback: S.fb});
    toast(t('sending'), 'info');
  };
  el('c-fbwrap').onclick = function() {
    S.fb = !S.fb; this.className = S.fb ? 'check on' : 'check'; this.style.padding = '7px 11px';
  };
  el('c-reconnect').onclick = function() { send('reconnect', {}); toast(t('sending'), 'info'); };
  el('c-test').onclick = function() { send('test_http', {}); };
  el('c-rawsend').onclick = function() { send('raw_ws', {payload: el('c-raw').value}); };
  el('c-gcodesend').onclick = function() { var c = el('c-gcode').value; if (c) send('gcode', {script: c}); };
  el('c-clear').onclick = function() { el('dlog').innerHTML = ''; send('clear_log', {}); };
}

if (window.orca && window.orca.onMessage) {
  window.orca.onMessage(function(d) {
    if (d.type === 'poll') {
      S.cursor = d.next;
      appendLog(d.log);
      S.status = d.status || {};
      S.motion = d.motion || {};
      S.caps = d.caps || {};
      S.runner = d.runner || {};
      S.media = d.media || {};
      renderControl(); renderRunner(); renderMotionState(); renderMediaState();
      renderCalib(false); renderMacros(false);
      if (d.rtc) rtcHandle(d.rtc);
      if (d.runner && d.runner.results) {
        var i, r;
        for (i = 0; i < d.runner.results.length; i++) {
          r = d.runner.results[i];
          if (r.ts > S.runSeen) {
            S.runSeen = r.ts;
            toast(r.label + ' \\u2192 ' + (r.ok ? t('done') : r.detail), r.ok ? 'ok' : 'err');
          }
        }
      }
      if (d.thumbs) applyThumbs(d.thumbs);
      if (d.files_version !== S.fv) { S.fv = d.files_version; send('files_get', {}); }
      if (d.history_version !== S.hv) { S.hv = d.history_version; send('history_get', {}); }
      if (d.camera) {
        var prevUrl = S.cam.url, prevProbe = S.cam.probing, prevState = S.cam.state;
        var prevSnaps = (S.cam.snaps || []).length;
        S.cam = d.camera;
        if (S.cam.snap_result && S.cam.snap_result.ts > S.snapSeen) {
          S.snapSeen = S.cam.snap_result.ts;
          if (S.cam.snap_result.ok) { shutter(); toast(t('cam_saved') + ': ' + S.cam.snap_result.name, 'ok'); }
          else toast(t('cam_error'), 'err');
        }
        if (prevUrl !== S.cam.url || prevProbe !== S.cam.probing || (S.cam.snaps || []).length !== prevSnaps) {
          renderCamSources(); renderSnaps();
        }
        if (S.tab === 'camera') {
          if (prevState !== S.cam.state && S.camEff === 'relay') {
            if (S.cam.state === 'error') camMessage('error');
            else if (S.cam.state === 'connecting') camMessage('connecting');
          }
          renderCamStats();
        }
      }
    } else if (d.type === 'cam_frame') {
      S.cam.state = d.state; S.cam.err = d.err; S.cam.fps = d.fps;
      S.cam.kbps = d.kbps; S.cam.uptime = d.uptime;
      if (d.jpeg && d.seq !== S.cam.seq) {
        S.cam.seq = d.seq;
        if (S.tab === 'camera' && S.camEff === 'relay' && !S.viewSnap) el('cam-img').src = d.jpeg;
      }
      if (d.state === 'error' && S.camEff === 'relay') camMessage('error');
      renderCamStats();
    } else if (d.type === 'cam_snap_img') {
      S.viewSnap = d.jpeg;
      clearDirectTimer(); rtcStop(); send('cam_stop', {});
      S.camEff = 'still';
      el('cam-video').style.display = 'none';
      el('cam-img').style.display = 'block';
      el('cam-img').src = d.jpeg;
      el('cam-back').style.display = 'inline-flex';
      camMessage('hide'); renderCamStats();
    } else if (d.type === 'files') {
      S.files = d.list || [];
      S.fv = d.version;
      var present = {}, i;
      for (i = 0; i < S.files.length; i++) present[S.files[i].name] = 1;
      for (var k in S.hidden) { if (S.hidden.hasOwnProperty(k) && !present[k]) delete S.hidden[k]; }
      el('cnt-files').textContent = S.files.length ? String(S.files.length) : '';
      if (S.tab === 'files') renderFiles(); else renderChips();
    } else if (d.type === 'history') {
      S.history = d.list || [];
      S.hstats = d.stats || {};
      S.hv = d.version;
      el('cnt-hist').textContent = S.history.length ? String(S.history.length) : '';
      if (S.tab === 'history') renderHistory();
    } else if (d.type === 'result') {
      if (d.action === 'silent') return;
      if (d.success) {
        toast(t('sent') + ' ' + d.detail, 'ok');
        if (d.action === 'print') showTab('control');
      } else toast(t('fail') + ': ' + d.detail, 'err');
    }
  });
}

window.addEventListener('load', function() {
  applyLang();
  bind();
  el('c-fbwrap').className = 'check on';
  showTab('control');
  showSub('main');
  renderDetail(); renderHDetail(); renderCamSources(); renderSnaps(); renderMotionState();
  send('poll', {cursor: 0, motion: true});
  setInterval(function() {
    var want = S.tab === 'files' ? requestThumbs() : [];
    send('poll', {cursor: S.cursor, thumbs: want, motion: S.tab === 'control'});
  }, 1000);
  setInterval(camTick, 220);
  setInterval(function() { if (S.tab === 'camera') renderCamStats(); }, 1000);
});
</script>
</body>
</html>"""

        html = html.replace("__LANG__", lang)
        html = html.replace("__TRANSLATIONS__", t_json)
        html = html.replace("__CAMCFG__", cam_json)
        html = html.replace("__IP__", str(CFG["ip"]))
        html = html.replace("__PORT__", str(CFG["ws_port"]))
        html = html.replace("__PATH__", str(CFG["ws_path"]))
        html = html.replace("__SILENT__", str(to_int(CFG["silent_pct"], 25)))
        return html

    def reply(self, success, detail, action=""):
        self.post_message({"type": "result", "success": bool(success),
                           "detail": str(detail), "action": action})

    def queued(self, result):
        ok, detail = result
        self.reply(ok, detail, "silent" if ok else "")

    def on_message(self, data):
        action = data.get("action")
        payload = data.get("data", {}) or {}

        if action == "ui_lang":
            want = normalize_lang(payload.get("lang"))
            if want:
                CFG["ui_lang"] = want
                save_cfg()
                try:
                    cur = json.loads(self.get_config() or "{}")
                    if not isinstance(cur, dict):
                        cur = {}
                except Exception:
                    cur = {}
                cur["language"] = want
                try:
                    self.save_config(json.dumps(cur))
                except Exception as exc:
                    log("ui_lang: save_config failed: " + str(exc)[:80])
                log("ui_lang -> " + want)
            return orca.ExecutionResult.success()
        if action == "jslog":
            log("JS: " + str(payload.get("msg", ""))[:1200])
            return orca.ExecutionResult.success()

        if action == "poll":
            ensure_thread()
            ensure_motion_thread()
            flush_pending()
            with LOCK:
                LINK["want_motion"] = bool(payload.get("motion", False))
            ensure_caps(False)
            try:
                cursor = int(payload.get("cursor", 0))
            except Exception:
                cursor = 0
            with LOCK:
                base = LINK["log_base"]
                start = cursor - base
                if start < 0:
                    start = 0
                if start > len(LINK["log"]):
                    start = len(LINK["log"])
                lines = LINK["log"][start:]
                nxt = base + len(LINK["log"])
                fv = LINK.get("files_version", 0)
                hv = LINK.get("history_version", 0)
            self.post_message({
                "type": "poll", "status": snapshot(), "log": lines, "next": nxt,
                "files_version": fv, "history_version": hv,
                "thumbs": collect_thumbs(payload.get("thumbs")),
                "camera": cam_public(), "motion": motion_public(),
                "caps": caps_public(), "runner": runner_public(),
                "rtc": rtc_public(), "media": media_public(),
            })
            return orca.ExecutionResult.success()

        if action == "rtc_offer":
            ok = rtc_offer(str(payload.get("body", "")), to_int(payload.get("seq", 0)))
            if not ok:
                self.reply(False, "RTC busy")
            return orca.ExecutionResult.success()

        if action == "media_probe":
            ok, info = media_probe()
            self.reply(ok, info)
            return orca.ExecutionResult.success()

        if action == "media_test":
            ok = media_test(str(payload.get("url", "")))
            self.reply(ok, "see log")
            return orca.ExecutionResult.success()

        if action == "jog":
            self.queued(jog_move(payload.get("axis"), payload.get("dist"), payload.get("speed")))
            return orca.ExecutionResult.success()

        if action == "home":
            self.queued(home_axes(payload.get("axes", "")))
            return orca.ExecutionResult.success()

        if action == "park":
            self.queued(park_move(payload.get("what", "")))
            return orca.ExecutionResult.success()

        if action == "z_offset":
            self.queued(z_offset(payload.get("delta", 0), to_int(payload.get("reset", 0))))
            return orca.ExecutionResult.success()

        if action == "motors_off":
            self.queued(motors_off())
            return orca.ExecutionResult.success()

        if action == "extrude":
            self.queued(extrude_move(payload.get("length", 0), payload.get("speed", 5)))
            return orca.ExecutionResult.success()

        if action == "filament":
            self.queued(filament_macro(payload.get("kind", "load")))
            return orca.ExecutionResult.success()

        if action == "flow":
            self.queued(flow_factor(payload.get("pct", 100)))
            return orca.ExecutionResult.success()

        if action == "calib":
            self.queued(calib_run(payload.get("kind", ""), payload.get("target", 0)))
            return orca.ExecutionResult.success()

        if action == "macro":
            self.queued(macro_run(payload.get("name", "")))
            return orca.ExecutionResult.success()

        if action == "caps_refresh":
            ensure_caps(bool(payload.get("force", False)))
            return orca.ExecutionResult.success()

        if action == "cam_frame":
            seq = to_int(payload.get("seq", 0))
            with LOCK:
                cur = CAM["seq"]
                jpg = CAM["jpeg"] if cur != seq else None
                info = {"state": CAM["state"], "err": CAM["err"],
                        "fps": round(CAM["fps"], 1), "kbps": round(CAM["kbps"], 1),
                        "uptime": int(time.time() - CAM["started"]) if CAM["started"] and CAM["state"] == "live" else 0}
            msg = {"type": "cam_frame", "seq": cur, "jpeg": None}
            msg.update(info)
            if jpg and base64 is not None:
                msg["jpeg"] = "data:image/jpeg;base64," + base64.b64encode(jpg).decode("ascii")
            self.post_message(msg)
            return orca.ExecutionResult.success()

        if action == "cam_cfg":
            patch = payload.get("patch", {}) or {}
            allowed = ("cam_url", "cam_kind", "cam_mode", "cam_fps", "cam_zoom", "cam_rot",
                       "cam_fliph", "cam_flipv", "cam_bright", "cam_contrast", "cam_sat",
                       "cam_grid", "cam_hud", "sound", "last_heat", "rtc_port", "rtc_path",
                       "rtc_h264_first")
            for key in allowed:
                if key in patch:
                    CFG[key] = patch[key]
            CFG["cam_fps"] = clamp(to_int(CFG["cam_fps"], 8), 1, 15)
            CFG["cam_zoom"] = clamp(to_int(CFG["cam_zoom"], 100), 100, 300)
            CFG["cam_rot"] = to_int(CFG["cam_rot"], 0) % 360
            CFG["cam_bright"] = clamp(to_int(CFG["cam_bright"], 100), 40, 200)
            CFG["cam_contrast"] = clamp(to_int(CFG["cam_contrast"], 100), 40, 200)
            CFG["cam_sat"] = clamp(to_int(CFG["cam_sat"], 100), 0, 220)
            CFG["last_heat"] = clamp(to_int(CFG["last_heat"], 200), 150, 320)
            CFG["rtc_port"] = clamp(to_int(CFG["rtc_port"], 8000), 1, 65535)
            save_cfg()
            return orca.ExecutionResult.success()

        if action == "cam_start":
            if CFG.get("cam_url"):
                cam_start_stream()
            else:
                with LOCK:
                    CAM["want"] = True
                cam_detect()
            return orca.ExecutionResult.success()

        if action == "cam_stop":
            cam_stop_stream()
            return orca.ExecutionResult.success()

        if action == "cam_detect":
            cam_detect()
            return orca.ExecutionResult.success()

        if action == "cam_snap":
            cam_snapshot()
            return orca.ExecutionResult.success()

        if action == "cam_snap_webrtc":
            jpeg_data = str(payload.get("jpeg", ""))
            if not jpeg_data.startswith("data:image/jpeg;base64,"):
                return orca.ExecutionResult.success()
            try:
                b64 = jpeg_data[len("data:image/jpeg;base64,"):]
                raw = base64.b64decode(b64)
                folder = snap_dir()
                if not os.path.isdir(folder):
                    os.makedirs(folder)
                name = "K1C_" + time.strftime("%Y%m%d_%H%M%S") + ".jpg"
                f = open(os.path.join(folder, name), "wb")
                f.write(raw)
                f.close()
                log("webrtc snapshot saved: " + name)
                # Feed the same result channel the MJPEG path uses, so the UI
                # plays the shutter and shows the "saved" toast on the next poll.
                with LOCK:
                    CAM["snap_result"] = {"ok": True, "name": name, "ts": time.time()}
                    CAM["snap_busy"] = False
                self.post_message({"type": "cam_snap_img", "name": name, "jpeg": jpeg_data})
            except Exception as e:
                log("webrtc snapshot save failed: " + str(e)[:80])
                with LOCK:
                    CAM["snap_result"] = {"ok": False, "name": "", "ts": time.time()}
                    CAM["snap_busy"] = False
            return orca.ExecutionResult.success()

        if action == "cam_snap_view":
            name = str(payload.get("name", ""))
            if "/" in name or "\\" in name or not name:
                return orca.ExecutionResult.success()
            try:
                f = open(os.path.join(snap_dir(), name), "rb")
                data = f.read()
                f.close()
            except Exception:
                data = None
            if data and base64 is not None:
                self.post_message({"type": "cam_snap_img", "name": name,
                                   "jpeg": "data:image/jpeg;base64," + base64.b64encode(data).decode("ascii")})
            return orca.ExecutionResult.success()

        if action == "cam_snap_del":
            name = str(payload.get("name", ""))
            if "/" in name or "\\" in name or not name:
                return orca.ExecutionResult.success()
            try:
                os.remove(os.path.join(snap_dir(), name))
            except Exception as e:
                log("snapshot delete: " + str(e)[:60])
            return orca.ExecutionResult.success()

        if action == "files_get":
            with LOCK:
                items = list(LINK.get("files", []))
                version = LINK.get("files_version", 0)
            self.post_message({"type": "files", "list": items, "version": version})
            return orca.ExecutionResult.success()

        if action == "files_refresh":
            self.reply(ws_queue("get", {"reqGcodeFile": 1}), "WebSocket")
            return orca.ExecutionResult.success()

        if action == "history_get":
            p = history_payload()
            p["type"] = "history"
            self.post_message(p)
            return orca.ExecutionResult.success()

        if action == "file_print":
            ok, detail = start_print(payload.get("path", ""), to_int(payload.get("calibrate", 1)))
            self.reply(ok, detail, "print")
            return orca.ExecutionResult.success()

        if action == "file_delete":
            path = payload.get("path", "")
            if path:
                queue_delete("file", path, payload.get("name", ""))
            return orca.ExecutionResult.success()

        if action == "file_undo":
            names = payload.get("names", []) or []
            with LOCK:
                LINK["pending"] = [i for i in LINK["pending"]
                                   if not (i["kind"] == "file" and i["name"] in names)]
            return orca.ExecutionResult.success()

        if action == "history_delete":
            queue_delete("history", payload.get("id"), payload.get("name", ""))
            with LOCK:
                LINK["history"] = [h for h in LINK["history"] if h.get("id") != payload.get("id")]
                LINK["history_version"] += 1
            return orca.ExecutionResult.success()

        if action == "history_undo":
            target = str(payload.get("id"))
            with LOCK:
                LINK["pending"] = [p for p in LINK["pending"] if str(p["target"]) != target]
            ws_queue("get", {"reqHistory": 1})
            return orca.ExecutionResult.success()

        if action == "print_cmd":
            self.queued(print_command(payload.get("cmd", "")))
            return orca.ExecutionResult.success()

        if action == "control":
            ok, detail = apply_control(payload.get("kind", ""), payload.get("value", 0))
            self.reply(ok, detail)
            return orca.ExecutionResult.success()

        if action == "motion_cfg":
            CFG["silent_pct"] = clamp(to_int(payload.get("value", 25), 25), 10, 100)
            save_cfg()
            self.reply(True, "silent {v}%".format(v=CFG["silent_pct"]))
            return orca.ExecutionResult.success()

        if action == "reconnect":
            ensure_thread()
            LINK["force_reconnect"] = True
            self.reply(True, "reconnect")
            return orca.ExecutionResult.success()

        if action == "config":
            CFG["ip"] = str(payload.get("ip", CFG["ip"])).strip()
            CFG["ws_port"] = to_int(payload.get("ws_port", CFG["ws_port"]), 9999)
            path = str(payload.get("ws_path", CFG["ws_path"])).strip() or "/"
            if path[0] != "/":
                path = "/" + path
            CFG["ws_path"] = path
            CFG["http_fallback"] = bool(payload.get("http_fallback", True))
            CFG["cam_url"] = ""
            CFG["media_base"] = ""
            save_cfg()
            with LOCK:
                THUMB["cache"] = {}
                THUMB["fail"] = {}
                THUMB["queue"] = []
                CAM["sources"] = []
                LINK["caps"] = {}
                LINK["motion"] = {}
                LINK["files_version"] += 1
                LINK["history_version"] += 1
            cam_stop_stream()
            ensure_thread()
            LINK["force_reconnect"] = True
            self.reply(True, "config")
            return orca.ExecutionResult.success()

        if action == "raw_ws":
            try:
                obj = json.loads(payload.get("payload", ""))
            except Exception as e:
                self.reply(False, "bad JSON: " + str(e)[:70])
                return orca.ExecutionResult.success()
            if not isinstance(obj, dict) or "method" not in obj:
                self.reply(False, "need object with method")
                return orca.ExecutionResult.success()
            if "params" not in obj:
                obj["params"] = {}
            self.reply(ws_queue(obj["method"], obj["params"]), "WebSocket")
            return orca.ExecutionResult.success()

        if action == "gcode":
            script = str(payload.get("script", ""))
            self.queued(gcode_async(script, script.split("\n")[0][:44], 300))
            return orca.ExecutionResult.success()

        if action == "test_http":
            try:
                info = http_json("/printer/info", 3)
                self.reply(True, "Moonraker " + str(info.get("result", {}).get("hostname", "?")))
            except Exception as e:
                self.reply(False, str(e)[:80])
            return orca.ExecutionResult.success()

        if action == "clear_log":
            with LOCK:
                LINK["log"] = []
                LINK["log_base"] = 0
            return orca.ExecutionResult.success()

        return orca.ExecutionResult.skipped()

@orca.plugin
class K1CControllerPlugin(orca.base):
    def register_capabilities(self):
        orca.register_capability(K1CControlPage)

# ---------------------------------------------------------------------------
# Native tab icon for the K1C Hub page.
# The SVG is embedded as text: no network access, no downloads, no external
# asset files. Works fully offline and cannot fail on a missing resource.
# Host signature: get_icon(self) -> str, same markup-as-string convention
# as get_ui() / get_config_ui().
# ---------------------------------------------------------------------------
K1C_TAB_ICON_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24">'
    '<rect x="2.5" y="3" width="19" height="2.4" rx="1.2" fill="#8C8C8C"/>'
    '<rect x="2.5" y="3" width="2.4" height="18" rx="1.2" fill="#8C8C8C"/>'
    '<rect x="19.1" y="3" width="2.4" height="18" rx="1.2" fill="#8C8C8C"/>'
    '<rect x="4.9" y="8.2" width="14.2" height="1.8" rx="0.9" fill="#8C8C8C"/>'
    '<path d="M10 10.4 h4 v2.6 l-2 2.8 l-2 -2.8 z" fill="#8C8C8C"/>'
    '<rect x="5.6" y="17.6" width="12.8" height="2.4" rx="1.2" fill="#8C8C8C"/>'
    '</svg>'
)


def _k1c_write_icon_file():
    """Materialise the embedded icon on disk. Used only as a path fallback."""
    try:
        folder = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(folder, "k1c_hub.svg")
        f = open(path, "w", encoding="utf-8")
        f.write(K1C_TAB_ICON_SVG)
        f.close()
        return path
    except Exception:
        return ""


K1C_TAB_ICON_PATH = _k1c_write_icon_file()


def _k1c_page_get_icon(self):
    return K1C_TAB_ICON_PATH


try:
    K1CControlPage.get_icon = _k1c_page_get_icon
except Exception:
    pass

# ---------------------------------------------------------------------------
# UI language. Explicit user choice wins over the host locale.
# Persisted twice: CFG["ui_lang"] in k1c_config.json and the capability config
# exposed in the Plugins dialog. Accepted values: auto / en / ru / zh.
# ---------------------------------------------------------------------------
def normalize_lang(value):
    v = str(value or "").strip().lower()
    return v if v in ("en", "ru", "zh", "auto") else ""


def detect_host_lang():
    """Read the host UI language and log what it actually reports."""
    raw = ""
    try:
        info = orca.host.app_info()
        if isinstance(info, dict):
            raw = info.get("ui_language", "")
        else:
            raw = getattr(info, "ui_language", "")
    except Exception as exc:
        log("lang: app_info unavailable: " + str(exc)[:80])
        return "en"
    text = str(raw or "").strip().lower()
    log("lang: host ui_language = '" + text[:40] + "'")
    if text.startswith("ru") or "russ" in text or "\u0440\u0443\u0441" in text:
        return "ru"
    if text.startswith("zh") or "chin" in text or "hans" in text or "\u4e2d" in text:
        return "zh"
    return "en"


def resolve_ui_lang(page):
    want = ""
    try:
        stored = json.loads(page.get_config() or "{}")
        if isinstance(stored, dict):
            want = normalize_lang(stored.get("language"))
    except Exception:
        want = ""
    if not want:
        want = normalize_lang(CFG.get("ui_lang"))
    if want and want != "auto":
        return want
    return detect_host_lang()


K1C_CFG_UI = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
* { box-sizing: border-box; }
body { margin: 0; padding: 18px; font-size: 13px;
font-family: var(--orca-font, "Segoe UI", sans-serif);
background: var(--orca-bg, #0b0d16); color: var(--orca-fg, #e9ecf8); }
.box { max-width: 540px; }
h1 { margin: 0 0 5px; font-size: 16px; display: flex; align-items: center; gap: 9px; }
h1 i { display: block; width: 4px; height: 16px; border-radius: 3px;
background: linear-gradient(180deg, #b18cff, #4fe3ff); }
.sub { font-size: 11.5px; line-height: 1.55; margin-bottom: 16px;
color: var(--orca-muted, #8d95b4); }
.lab { font-size: 10px; text-transform: uppercase; letter-spacing: 1.1px;
margin-bottom: 8px; color: var(--orca-muted, #8d95b4); }
.opts { display: flex; flex-direction: column; gap: 7px; margin-bottom: 16px; }
.opt { display: flex; align-items: center; gap: 11px; padding: 11px 13px;
border-radius: 11px; cursor: pointer; transition: all .15s;
border: 1px solid var(--orca-border, #232842); background: rgba(255,255,255,0.04); }
.opt:hover { border-color: rgba(177,140,255,0.55); }
.opt.on { border-color: #b18cff; background: rgba(177,140,255,0.14); }
.rad { width: 16px; height: 16px; border-radius: 50%; flex: none;
border: 2px solid #5a6180; position: relative; }
.opt.on .rad { border-color: #b18cff; }
.opt.on .rad:after { content: ""; position: absolute; inset: 3px; border-radius: 50%;
background: linear-gradient(135deg, #b18cff, #4fe3ff); }
.nm { flex: 1; font-size: 12.5px; font-weight: 600; }
.tag { font-size: 9.5px; font-weight: 800; letter-spacing: 1px; text-transform: uppercase;
color: var(--orca-muted, #8d95b4); }
.foot { display: flex; align-items: center; gap: 12px; }
.btn { border: none; border-radius: 9px; padding: 9px 18px; font-size: 12.5px;
font-weight: 700; cursor: pointer; font-family: inherit; color: #fff;
background: linear-gradient(135deg, #7c5cff, #3b7bff); }
.btn:hover { filter: brightness(1.12); }
.btn:disabled { opacity: .45; cursor: default; filter: none; }
.msg { font-size: 11.5px; color: var(--orca-muted, #8d95b4); }
.msg.ok { color: #3fe08c; }
.msg.err { color: #ff4d5e; }
.note { margin-top: 14px; padding: 10px 12px; border-radius: 10px;
font-size: 11px; line-height: 1.5; background: rgba(79,227,255,0.07);
border: 1px solid rgba(79,227,255,0.26); color: var(--orca-muted, #8d95b4); }
</style></head><body><div class="box">
<h1><i></i><span>K1C Hub</span></h1>
<div class="sub" id="sub"></div>
<div class="lab" id="lab"></div>
<div class="opts" id="opts"></div>
<div class="foot"><button class="btn" id="save">Save</button><span class="msg" id="msg"></span></div>
<div class="note" id="note"></div>
</div>
<script>
var TXT = {
en: { sub: 'Interface language of the K1C Hub page.', lab: 'Language', save: 'Save',
saved: 'Saved. Reopen the plugin page to apply.', failed: 'Could not save the setting.',
note: 'Auto follows the OrcaSlicer interface language. Detected now: __HOSTLANG__.',
bridge: 'Configuration bridge is unavailable in this window.',
auto: 'Auto (follow OrcaSlicer)', en: 'English', ru: 'Russian', zh: 'Chinese' },
ru: { sub: 'Язык интерфейса страницы K1C Hub.', lab: 'Язык', save: 'Сохранить',
saved: 'Сохранено. Откройте страницу плагина заново.', failed: 'Не удалось сохранить настройку.',
note: 'Авто следует языку интерфейса OrcaSlicer. Сейчас определён: __HOSTLANG__.',
bridge: 'Мост конфигурации недоступен в этом окне.',
auto: 'Авто (как в OrcaSlicer)', en: 'Английский', ru: 'Русский', zh: 'Китайский' },
zh: { sub: 'K1C Hub 页面的界面语言。', lab: '语言', save: '保存',
saved: '已保存。请重新打开插件页面。', failed: '无法保存设置。',
note: '自动跟随 OrcaSlicer 界面语言。当前检测为：__HOSTLANG__。',
bridge: '此窗口中的配置桥接不可用。',
auto: '自动（跟随 OrcaSlicer）', en: '英语', ru: '俄语', zh: '中文' }
};
var LANGS = ['auto', 'en', 'ru', 'zh'];
var pick = 'auto';
var api = window.orca || {};

function el(id) { return document.getElementById(id); }
function mk(tag, cls, text) {
var e = document.createElement(tag);
if (cls) e.className = cls;
if (text !== undefined && text !== null) e.appendChild(document.createTextNode(text));
return e;
}
function txt(k) { var d = TXT[pick] || TXT.en; return d[k] || TXT.en[k] || k; }

function paint() {
var box = el('opts'), i;
el('sub').textContent = txt('sub');
el('lab').textContent = txt('lab');
el('save').textContent = txt('save');
el('note').textContent = txt('note');
box.innerHTML = '';
for (i = 0; i < LANGS.length; i++) {
(function(code) {
var row = mk('div', 'opt' + (pick === code ? ' on' : ''));
row.appendChild(mk('div', 'rad'));
row.appendChild(mk('div', 'nm', txt(code)));
row.appendChild(mk('div', 'tag', code === 'auto' ? '' : code));
row.onclick = function() { pick = code; paint(); };
box.appendChild(row);
})(LANGS[i]);
}
}

/* The bridge may be sync or promise-based; support both shapes. */
function parseCfg(v) {
if (!v) return {};
if (typeof v === 'string') { try { return JSON.parse(v) || {}; } catch (e) { return {}; } }
if (typeof v === 'object') return v;
return {};
}
function loadCfg(done) {
var r;
if (!api.getConfig) { done({}); return; }
try { r = api.getConfig(); } catch (e) { done({}); return; }
if (r && typeof r.then === 'function') {
r.then(function(v) { done(parseCfg(v)); })['catch'](function() { done({}); });
} else { done(parseCfg(r)); }
}
function saveCfg(obj, done) {
var r;
if (!api.saveConfig) { done(false); return; }
try { r = api.saveConfig(JSON.stringify(obj)); }
catch (e) {
try { r = api.saveConfig(obj); } catch (e2) { done(false); return; }
}
if (r && typeof r.then === 'function') {
r.then(function(v) { done(v !== false); })['catch'](function() { done(false); });
} else { done(r !== false); }
}

function init() {
paint();
loadCfg(function(cfg) {
var v = String(cfg.language || 'auto').toLowerCase();
pick = LANGS.indexOf(v) >= 0 ? v : 'auto';
paint();
if (!api.saveConfig) {
el('msg').className = 'msg err';
el('msg').textContent = txt('bridge');
el('save').disabled = true;
}
});
el('save').onclick = function() {
var btn = this;
btn.disabled = true;
loadCfg(function(cfg) {
cfg.language = pick;
saveCfg(cfg, function(ok) {
btn.disabled = false;
el('msg').className = 'msg ' + (ok ? 'ok' : 'err');
el('msg').textContent = ok ? txt('saved') : txt('failed');
});
});
};
}
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
</script>
</body></html>"""


def _k1c_has_config_ui(self):
    return True


def _k1c_get_config_ui(self):
    names = {"en": "English", "ru": "Russian", "zh": "Chinese"}
    return K1C_CFG_UI.replace("__HOSTLANG__", names.get(detect_host_lang(), "English"))


try:
    K1CControlPage.has_config_ui = _k1c_has_config_ui
    K1CControlPage.get_config_ui = _k1c_get_config_ui
except Exception:
    pass
