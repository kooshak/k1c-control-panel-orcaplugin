# K1C Hub

> Full control of your **Creality K1C** 3D printer directly from **OrcaSlicer** — no Creality Print, Fluidd, or Mainsail required.

[![Version](https://img.shields.io/badge/version-0.9.0-blue)](https://github.com/kczk/k1c-hub)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)
[![OrcaSlicer](https://img.shields.io/badge/OrcaSlicer-2.5.0%2B-orange)](https://github.com/SoftFever/OrcaSlicer)
[![Printer](https://img.shields.io/badge/Printer-Creality%20K1C-purple)](https://www.creality.com/)

**K1C Hub** is a single-file plugin for OrcaSlicer that implements the native Creality K1C protocol. It communicates directly with the printer via WebSocket (port 9999) and falls back to Moonraker HTTP (port 7125), supporting WebRTC camera, print control, file management, history, motion control, and calibration.

---

## 📸 Screenshots

<img width="2538" height="1264" alt="изображение_2026-11-13_102615792" src="https://github.com/user-attachments/assets/9ae90ea9-9c2d-4a89-9b26-c4ae3c584131" />

<img width="2545" height="1255" alt="изображение_2026-11-13_102339383" src="https://github.com/user-attachments/assets/f4ca49ad-f1fc-4452-92f4-38e96dc984f2" />

<img width="2559" height="1269" alt="image" src="https://github.com/user-attachments/assets/4954f818-52aa-424e-878a-f829ce4fd2bc" />

<img width="2544" height="1265" alt="изображение_2026-11-13_102122774" src="https://github.com/user-attachments/assets/7b767636-5b5e-4f44-a5bf-fb29baba91b6" />

<img width="2545" height="1259" alt="image" src="https://github.com/user-attachments/assets/e3219ec7-9557-4751-958b-d3c68e94247a" />




## ✨ Features

### 🎛 Control
- **Live telemetry** — nozzle, bed, chamber, position, print status in real-time
- **Temperatures** — nozzle (0–320°C), bed (0–115°C), chamber, quick presets for PLA/PETG/ASA/TPU
- **Fans** — model fan, chassis fan, side fan (0–100%)
- **LED light** — on/off control
- **Motion modes** — Silent / Stable / Standard / Hyper

### 🎥 Camera
- **WebRTC** — native stream via signaling on port 8000 (H.264, low-latency)
- **MJPEG** — direct and relay modes for compatibility
- **Settings** — zoom, rotation, H/V flip, brightness, contrast, saturation
- **Overlays** — HUD with telemetry, rule-of-thirds grid
- **Snapshots** — save JPEG to plugin folder with preview and delete

### 📁 Files
- Thumbnails with lazy loading
- Search, sorting (date/name/size/duration)
- Material filters (PLA, PETG, ASA, ABS, TPU, PA-CF, PC, PET)
- Bulk selection and delete with 6-second undo window
- Detailed panel with slicer metadata

### 📊 History
- Statistics: jobs, print time, filament usage
- Timelapse viewer (if media source found)
- Auto-discovery of media server (ports 80/8081/7125 × prefixes × strip variants)
- Entry deletion with undo

### 🎯 Motion
- XY jog pad + Z control
- Homing (all / XY / Z)
- Parking (Z to bottom, center head)
- Z-offset / babystep (±0.01, ±0.05)
- Motor disable with confirmation
- **Motion lock** — blocks manual control during printing

### 🔧 Calibration
- Bed mesh (BED_MESH_CALIBRATE / CLEAR)
- Input shaping (SHAPER_CALIBRATE)
- PID nozzle / PID bed
- SAVE_CONFIG with confirmation
- Run arbitrary printer macros with filter

### 🛡 Safety
- Motion lock during printing/calibration
- Confirmations for dangerous actions (stop, motors off, save config)
- can_extrude check before extrusion
- homed_axes check before motion
- Range validation before jog

---

## Architecture

### Primary: Creality WebSocket (port 9999)
- Custom RFC 6455 implementation using pure sockets (`socket` + `struct` + `base64`)
- No external dependencies
- Push notifications for status, temperatures, errors
- `set`/`get` commands via JSON
- Auto-reconnect with backoff (1s → 10s)
- 12-second grace period after connection loss

### Fallback: Moonraker HTTP (port 7125)
- Activates automatically after 8 seconds of WS idle
- REST queries to Klipper objects
- G-code sending via `/printer/gcode/script`
- File metadata and thumbnails via `/server/files/metadata`

### Camera: WebRTC (port 8000)
- Signaling via `/call/webrtc_local`
- SDP exchange in base64
- H.264 priority via `setCodecPreferences`
- ICE candidates via STUN (Google)
- Fallback to MJPEG if WebRTC unavailable

---

## 📋 Requirements

- **OrcaSlicer** 2.5.0 or newer
- **Printer**: Creality K1C 2025 (Klipper + Moonraker + DWIN firmware)
- **Network**: printer and computer on the same subnet


---

## 📥 Installation

1. Download `main.py` from [Releases](../../releases) or clone the repository
2. Go to the plugins folder for Orca Slicer at the following path: **Windows**: `AppData\Roaming\OrcaSlicer\orca_plugins`
3. Create a folder named "K1C Controller" (or any other name you prefer).
4. Copy the main.py file into the created folder.
5. Restart OrcaSlicer
6. Open the plugin tab and eneable plugin
7. In the plugin settings, enable full access to avoid having permission prompts pop up every few seconds.

---

## 🚀 First Launch

1. Open the **K1C Hub** tab
2. Click **Diagnostics** (top right)
3. Enter your printer IP (e.g., `192.168.1.100`)
4. Click **Save & reconnect**
5. Wait for the green **WebSocket** badge in the header
6. Set up the camera: **Camera** tab → Transport: **WebRTC** → click **Stream**

> 💡 The IP is saved in `k1c_config.json` next to `main.py` and loaded automatically on subsequent launches.


---

## ❓ FAQ

### Printer won't connect
- Check that the printer and computer are on the same subnet
- Make sure the IP in Diagnostics is correct
- Try the **Reconnect** button
- Check **Test HTTP** — Moonraker should respond

### Camera doesn't show video
- Make sure the camera is physically connected to the printer
- In camera settings, select Transport: **WebRTC**
- Default WebRTC port: **8000**

### How to find the printer IP?
- On the printer screen: **Settings → Network → IP address**
- Or via your router: find the `K1C` device in the client list

### Can I use it with K1 / K1 Max?
The plugin was tested on K1C 2025. K1 and K1 Max have a similar protocol but may differ in endpoints. Use at your own risk.

---

## Known Limitations

- CFS (Creality Filament System) is not supported
- Multi-color printing is not supported
- Creality cloud features are not integrated
- Only one printer per installation (no device switching)

---


- [OrcaSlicer](https://github.com/SoftFever/OrcaSlicer) — for the Plugin API
- [Creality](https://www.creality.com/) — for K1C and the open protocol
- [Klipper](https://www.klipper3d.org/) — for the firmware
- [Moonraker](https://moonraker.readthedocs.io/) — for the HTTP API

---
