# 📈 Trading Replay — Android App

Native Android app for replaying TradingView screenshot charts.
Data source: `https://raw.githubusercontent.com/sp33dz/tv-screenshot/refs/heads/main/gallery/data.json`

---

## ✅ Features

| Feature | Details |
|---------|---------|
| 📊 Chart Replay | เล่นภาพ chart ทีละเฟรม พร้อม speed control 0.25× – 8× |
| 🔍 Pinch-to-Zoom | Zoom ด้วย 2 นิ้ว, double-tap to zoom in/out |
| 👆 Touch Pan | ลาก pan ขณะ zoom |
| 🔎 Smart Preload | โหลดล่วงหน้า 5 ภาพเพื่อ smooth replay |
| 🎛️ Filters | กรองตาม Symbol / Market / Month / Tag |
| 🖼️ Gallery | ดูภาพทั้งหมด ค้นหา กดเลือกเพื่อ jump |
| 📝 Notes | บันทึก note + mark (Bull/Bear/Note) ต่อแต่ละภาพ |
| 🔄 Auto Refresh | refresh data ทุก 1 ชั่วโมงอัตโนมัติ |
| ⛶ Fullscreen | ซ่อน status/nav bar |
| ⌨️ Keyboard | Arrow keys, Space (play/pause), F (fullscreen) |
| 💾 Persistent Notes | notes บันทึกใน SharedPreferences (ไม่หายเมื่อปิด app) |

---

## 🔨 Build & Install APK

### Requirements
- Android Studio Hedgehog (2023.1.1) หรือใหม่กว่า
- JDK 17+
- Android SDK 34

### Step 1: Clone / Copy project
```
TradingReplayApp/
├── app/
│   ├── src/main/
│   │   ├── assets/replay.html     ← UI หลัก (แก้ได้)
│   │   ├── java/.../MainActivity.java
│   │   ├── res/layout/activity_main.xml
│   │   └── ...
│   └── build.gradle
├── settings.gradle
└── build.gradle
```

### Step 2: Open in Android Studio
1. Open Android Studio
2. **File → Open** → เลือกโฟลเดอร์ `TradingReplayApp`
3. รอ Gradle sync เสร็จ

### Step 3: Build APK
```bash
# Debug APK (ติดตั้งได้ทันที)
./gradlew assembleDebug

# APK อยู่ที่:
app/build/outputs/apk/debug/app-debug.apk
```

หรือใน Android Studio: **Build → Build Bundle(s)/APK(s) → Build APK(s)**

### Step 4: Install on device
```bash
# ผ่าน ADB
adb install app/build/outputs/apk/debug/app-debug.apk

# หรือ copy ไฟล์ .apk ไปที่มือถือ แล้วเปิดติดตั้ง
# (ต้องเปิด "Install unknown apps" ใน Settings ก่อน)
```

---

## 📱 การใช้งาน

1. **เปิด App** → ระบบโหลด data.json จาก GitHub อัตโนมัติ
2. **เลือก Symbol** จาก Filter (ปุ่ม ☰ หรือ tab 🎛️)
3. กด **▶ Play** เพื่อเริ่ม replay
4. **Swipe scrubber** เพื่อ jump ไปวันที่ต้องการ
5. **Pinch zoom** ดู chart ละเอียด
6. **Double tap** = zoom in/out toggle
7. **Tap overlay** = ซ่อน/แสดง info badge
8. **Tab Gallery** (🖼️) = ดูทุกภาพ ค้นหา กดเลือก jump

---

## 🔧 ปรับแต่ง

### เปลี่ยน Data URL
แก้ในไฟล์ `app/src/main/assets/replay.html` บรรทัด:
```javascript
const DATA_URL = 'https://raw.githubusercontent.com/.../data.json';
```

### เปลี่ยน Speed Options
```javascript
const SPEEDS = [0.25, 0.5, 1, 2, 4, 8];
```

### เปิด Debug WebView
เพิ่มใน `MainActivity.java`:
```java
WebView.setWebContentsDebuggingEnabled(true);
```
แล้วเปิด `chrome://inspect` ใน Chrome บนเครื่องที่ต่อ USB

---

## 📦 ไฟล์สำคัญ

| ไฟล์ | หน้าที่ |
|------|---------|
| `assets/replay.html` | UI ทั้งหมด (HTML/CSS/JS) — แก้ได้โดยไม่ต้อง rebuild |
| `MainActivity.java` | Android host, WebView setup, JavaScript bridge |
| `AndroidManifest.xml` | permissions (INTERNET), activity config |
| `app/build.gradle` | dependencies, compileSdk, minSdk |

---

## ⚠️ Notes

- **minSdk 24** (Android 7.0+) ครอบคลุม ~99% ของ Android ที่ใช้งานอยู่
- ต้องมี Internet connection เพื่อโหลดรูปภาพ
- Notes/annotations เก็บใน SharedPreferences ของ app
- รูปภาพใช้ cache ใน memory (ไม่เขียน disk)
