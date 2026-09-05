# sf_tree_reporting

This repository has moved to `tree_reporting`.

If you reached this repo through an old GitHub Pages link, it now redirects to the matching path on:

<https://greenmtnboy.github.io/tree_reporting/>

## How the redirect is served

`index.html` and `404.html` are static redirect shims. `404.html` is what makes deep
links work: GitHub Pages serves it for any path under `/sf_tree_reporting/` that has no
file, and its script strips the `/sf_tree_reporting` prefix and forwards the remaining
path, query string, and fragment to `/tree_reporting`.

Those files only do anything if GitHub Pages is actually publishing this repo.
`.github/workflows/pages.yml` handles that: it runs on every push to `main`, calls
`actions/configure-pages` with `enablement: true` to turn Pages on if it is off, and
deploys the repo root.

If the workflow fails to enable Pages (org policy can block `enablement`), turn it on
manually once under **Settings → Pages** and set the source to **GitHub Actions**; the
workflow handles every deploy after that.
