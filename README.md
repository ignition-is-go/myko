![Music](project-mascot.gif)

# [RocketShip] Frontend

Web Frontend Client.

## Install Dependencies

In the project directory, run:

```sh
npm install
```

This installs all the npm dependencies for the project.<br>

## Available Scripts

In the project directory, you can run:

### `npm run start-development`

Runs the app in the development mode.<br>
Open [http://localhost:4000](http://localhost:4000) to view it in the browser.

The page will reload if you make edits.<br>
You will also see any errors in the console.

### `npm run build`

Builds the app for production to the `.next` folder.<br>
It correctly bundles React in production mode and optimizes the build for the best performance.

### `npm run start-testing`
### `npm run start-production`

Starts the application in testing/production mode.
The application should be compiled with \`next build\` first.

See the section in Next docs about [deployment](https://github.com/zeit/next.js/wiki/Deployment) for more information.

## Server Side Rendering

For the initial page load, `getInitialProps` will execute on the server only. `getInitialProps` will only be executed on the client when navigating to a different route via the `Link` component or using the routing APIs.

_Note: `getInitialProps` can **not** be used in children components. Only in `pages`._

Read more about [fetching data and the component lifecycle](https://github.com/zeit/next.js#fetching-data-and-component-lifecycle)

## Environment Variables

The project consumes variables declared in your environment as if they were declared locally in your JS files. By default you will have `NODE_ENV` defined for you.

These environment variables will be defined for you on `process.env`. For example, having an environment
variable named `MY_VARIABLE` will be exposed in your JS as `process.env.MY_VARIABLE`.

`.env.*` files **should be** checked into source control (with the exclusion of `.env`).

#### What other `.env` files can be used?

* `.env`: Default, this one should not be commited to the repo. 
* `.env.development`: Development variables.
* `.env.testing`: Testing variables.
* `.env.production`: Production variables.

## Folder Structure

```
/
  README.md
  package.json
  next.config.js
  doc/
  pages/
    index.js
  src/
    components/
    containers/
  static/
    favicon.ico
  server.js
  .env
  Dockerfile
  bitbucket-pipelines.yml
  now.json
```

Routing in Next.js is based on the file system, so `./pages/index.js` maps to the `/` route and
`./pages/about.js` would map to `/about`.

The `./static` directory maps to `/static` in the `next` server, so you can put all your
other static resources like images or compiled CSS in there.

Out of the box, we get:

- Automatic transpilation and bundling (with webpack and babel)
- Hot code reloading
- Server rendering and indexing of `./pages`
- Static file serving. `./static/` is mapped to `/static/`

Read more about [Next's Routing](https://github.com/zeit/next.js#routing)

## Techs used

* [React.js](https://reactjs.org/).
* [Next.js](https://github.com/zeit/next.js/).
* [Create Next App](https://github.com/segmentio/create-next-app).
* [SASS](https://sass-lang.com/).
* [react-toolbox-themr](https://github.com/react-toolbox/react-toolbox-themr).
* [ESLint](https://eslint.org/).