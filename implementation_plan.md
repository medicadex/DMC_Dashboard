# Web-App wrapper Conversion Plan

We will transition the `DMC Management Suite` from standard `Tkinter` to a modern **Web-App Wrapper architecture** using **Flask** and **PyWebView**. This allows us to keep 100% of the backend Python logic intact while delivering a cutting-edge HTML/CSS/JS user interface.

Because this is a massive change, we must do it iteratively.

## User Review Required

> [!WARNING]
> This transition completely changes the UI of your application. While all data processing code remains identical, the exact functionality of every single screen (Dashboard, Uploader, Admin Report) has to be manually rebuilt in HTML/CSS/JavaScript. It will take time to reach 100% feature parity with the old Tkinter version.
> To proceed, please confirm you are okay with starting the port iteratively (beginning with the overarching Desktop Wrapper, Login Screen, and Dashboard).

## Phase 1: Core Wrapper Setup

We will create a new desktop entrypoint that wraps the Flask application in a Chromium window, mimicking a native desktop app.

### Web Engine Entrypoint

#### [NEW] `web_main.py`
A new script that initializes Flask and launches it within `pywebview`. It will have dynamic desktop window sizes and native OS controls while pointing internally to the Flask server.

#### [MODIFY] `requirements.txt`
Add `pywebview` and verify `flask` is ready.

### Core Structure Update

#### [MODIFY] `app.py`
We will rewrite `app.py` to properly import the existing `services/`, `repositories/`, and `db_utils.py` exactly as `main_app.py` did. It will expose a JSON REST API for the frontend rather than just basic HTML strings.

## Phase 2: Modernizing the Login & Dashboard (Initial Slice)

As our first working slice, we will recreate the Authentication and the Account Dashboard screens.

#### [NEW] `templates/base.html`
A master layout containing the dynamic Sidebar, Top Navigation, and the overarching "Layout" in CSS (Grid/Flexbox).

#### [NEW] `static/css/global.css`
A centralized styling sheet utilizing modern design standards (Glassmorphism, shadow depths, clean typography like 'Inter' or 'Roboto').

#### [NEW] `templates/login.html`
A sleek, modern login portal that uses Javascript `fetch()` APIs to communicate the credentials to `app.py`'s `/login` endpoint.

#### [MODIFY] `templates/dashboard.html`
An interactive dashboard displaying the financial summaries, using modern charting libraries (like Chart.js) and interactive tables instead of `Treeview`.

## Open Questions

1. **Which theme do you prefer?** A sleek "Dark Mode" native application feel, or a clean, bright, professional "Light Mode" (similar to modern banking software)?
2. **Would you like me to install PyWebView now** to act as the web-window wrapper, or do we want to build it entirely as a website you access in Google Chrome first? (I recommend PyWebView so it still feels like a desktop `.exe` app to your staff).

## Verification Plan

### Automated Checks
- Run `web_main.py` and verify a desktop application window spawns correctly (not opening an external browser).
- Test connection to the local database via the `/api/login` endpoint through the Javascript interface.

### Manual Verification
- You will need to test the visual responsiveness of the new login screen and ensure the application boots up properly on your Windows machine.
