# Live sync compared: Vectro, Hasura, Firestore

The same 1,000-product "revenue per product, sorted" answer kept live while
orders change; time from the write to the client having the change. Results:
`compare/results.json`, section `liveCompared` (Vectro alone: `live`).

- `bench-hasura.mjs`: Hasura + Postgres in Docker.
  ```
  docker network create liverivals
  docker run -d --name lr-pg --network liverivals -p 127.0.0.1:5442:5432 -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=shop postgres:16
  docker run -d --name lr-hasura --network liverivals -p 127.0.0.1:8085:8080 \
    -e HASURA_GRAPHQL_DATABASE_URL=postgres://postgres:bench@lr-pg:5432/shop \
    -e HASURA_GRAPHQL_LIVE_QUERIES_MULTIPLEXED_REFETCH_INTERVAL=1000 hasura/graphql-engine:latest
  node bench-hasura.mjs            # INTERVAL=100 when the container has 100
  ```
- `bench-firestore.mjs`: the Firestore emulator (needs Java 21):
  `npx firebase emulators:start --only firestore --project vectro-live-bench`, then `node bench-firestore.mjs`.

`npm install` here first (pg, firebase, firebase-tools). Run one at a time on
a quiet machine (load under 4), and stop the containers / emulator after.
