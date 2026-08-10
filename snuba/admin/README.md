# Running
You must have node and yarn installed. To do so:
```
volta install node
volta install yarn
make build-admin
```

To run the app locally:
```bash
# assuming you're in the venv
snuba admin
```

The server should be running on http://127.0.0.1:1219

note: from the Sentry repository, run `devservices up` and `devservices toggle snuba local` to start Snuba's dependencies for local development. Run `devservices toggle snuba containerized` to switch back to the containerized deployment.

# Developing the Javascript

To start the yarn debug server and live reload your javascript changes.
```
make watch-admin
```

 If you change environment variables you'll have to restart the server

# Releasing new javascript

The admin tool is automatically built as part of our normal CI flow.
