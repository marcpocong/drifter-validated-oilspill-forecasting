# UI Branding

The dashboard supports optional real branding assets, but it does not require them. If the logo files are absent, the app falls back to clean text-only branding and still loads normally.

## Where To Place The Files

Put branding assets in:

```text
ui/assets/
```

## Accepted Filenames

Main logo:

- `ui/assets/logo.svg`
- `ui/assets/logo.png`

Optional icon:

- `ui/assets/logo_icon.svg`
- `ui/assets/logo_icon.png`

The app checks those names directly, so replacing the branding later is as simple as swapping the files while keeping the filenames.

## How The App Uses Them

- `logo.svg` or `logo.png` is used for the main app branding.
- `logo_icon.png` is preferred for the browser/page icon when available.
- `logo_icon.svg` can still be used by the app branding layer even if the browser icon falls back.
- If only `logo.png` exists, the app may reuse it as the page icon.
- If no logo exists, the app falls back to text-only branding without showing broken image placeholders.

## Recommended Asset Shape

- Prefer a transparent background.
- Prefer a wide primary logo rather than a square one.
- A `3:1` to `5:1` aspect ratio works well for the main logo.
- A raster main logo around `1200 x 300` or `1600 x 400` is a safe target.
- Keep extra whitespace inside the file to a minimum so the logo does not look surrounded by empty margins.
- Keep the optional icon square.
- A `256 x 256` or `512 x 512` icon is a safe default.

## Replacement Steps

1. Replace `logo.svg` or `logo.png` with the new main logo.
2. Optionally replace `logo_icon.svg` or `logo_icon.png` with the matching icon.
3. Reload the Streamlit app.

No code change is needed as long as the filenames stay within the supported convention above.

## Fallback Behavior

- No logo files: the app shows text-only branding.
- Main logo only: the app still shows the logo and may reuse the raster logo as the page icon.
- Main logo plus icon: the app uses the main logo for branding and the icon for the browser tab where supported.
