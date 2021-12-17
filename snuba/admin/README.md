# Running

To run the app locally:

```bash
# assuming you're in the venv
snuba admin
```

The server should be running on http://localhost:1219

# Developing the Javascript

To change the javascript:

```
make watch-admin
```

this will start the yarn debug server and live reload your javascript changes. If you change environment variables you'll have to restart the server

# Releasing new javascript

At time of writing, we check the compiled javscript bundle into source code (yes it's not great, if you want to fix it please do). Run this before checking in your JS changes:

```
make build-admin
```
