# RshipClient

## Satellite 
Satellite is an electron + angular project that works with rocketship

### Dev Notes

Any main thread dependencies for satellite need to exist both in the dev dependencies of the top level `./package.json`, as well as the dependencies of the `apps/satellite/package.json` in order to both debug and package properly.




This project was generated using [Nx](https://nx.dev).

🔎 **Nx is a set of Angular CLI power-ups for modern development.**

## Development server

Run `ng serve my-app` for a dev server. Navigate to http://localhost:4200/. The app will automatically reload if you change any of the source files.

## Code scaffolding

Run `ng g component my-component --project=my-app` to generate a new component.

## Build

Run `ng build my-app` to build the project. The build artifacts will be stored in the `dist/` directory. Use the `--prod` flag for a production build.

## Running unit tests

Run `ng test my-app` to execute the unit tests via [Jest](https://jestjs.io).

Run `npm run affected:test` to execute the unit tests affected by a change.

## Running end-to-end tests

Run `ng e2e my-app` to execute the end-to-end tests via [Cypress](https://www.cypress.io).

Run `npm run affected:e2e` to execute the end-to-end tests affected by a change.

## Understand your workspace

Run `npm run dep-graph` to see a diagram of the dependencies of your projects.



# Local Cache
GraphQL is used for local cache store. Default way for updating cache is refetch query, you can use generated refetchQuery from `graph.service.ts`
