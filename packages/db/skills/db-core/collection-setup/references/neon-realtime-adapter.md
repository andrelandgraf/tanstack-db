# Neon Realtime Adapter Reference

Neon Realtime keeps the result of a read-only SQL query on Lakebase Postgres
live. `@neon/realtime-tanstack` syncs that result into a keyed TanStack DB
collection. Any read-only query works, including joins, aggregates, window
functions and subqueries. Writes go through your own server functions.

For the full walkthrough with `neon.ts` setup and a complete TanStack Start
app, fetch https://neon.com/docs/tanstack/realtime-db.md.

Realtime is in beta and needs a Neon project in a supported region. Enabling
it permanently turns on logical replication for the project and suspends its
computes on every branch, which drops open connections. Tell the user before
enabling it on an existing project. See https://neon.com/docs/realtime/overview.

## Install

```bash
npm install @neon/realtime @neon/realtime-tanstack @tanstack/react-db
npm install @tanstack/react-router-with-db # TanStack Start SSR only
```

`@tanstack/react-router-with-db` requires `@tanstack/react-db` 1.0.0-rc.1 or
later.

| Variable                 | Where       |
| ------------------------ | ----------- |
| `DATABASE_URL`           | server only |
| `NEON_REALTIME_SECRET`   | server only |
| `VITE_NEON_REALTIME_URL` | browser     |

`neon deploy` with `realtime` in `neon.ts` writes `DATABASE_URL`,
`NEON_REALTIME_URL` and `NEON_REALTIME_SECRET` to `.env.local`. Never give
`NEON_REALTIME_SECRET` a `VITE_` prefix.

## Server: define and seal the query

```ts
// src/todos.server.ts
import { createRealtime, rawSql } from '@neon/realtime/server'

export const realtime = createRealtime({
  secret: process.env.NEON_REALTIME_SECRET!,
  db: 'neondb',
})

export type Todo = { id: string; title: string; completed: boolean }

export const TODOS_SQL =
  'SELECT id, title, completed FROM todos WHERE user_id = $1'

export const todosQuery = (userId: string) => rawSql<Todo>(TODOS_SQL, [userId])
```

`requirePageUser` checks the session and rejects a page loaded for a different
user (for example after signing in as someone else in another tab):

```ts
// src/page-user.functions.ts
import { createMiddleware } from '@tanstack/react-start'
import { setResponseStatus } from '@tanstack/react-start/server'
import { z } from 'zod'
import { requireUser } from './auth.functions'

// Loose, so each server function's own fields reach its validator.
export const requirePageUser = createMiddleware({ type: 'function' })
  .middleware([requireUser])
  .validator(z.looseObject({ userId: z.string() }))
  .server(({ next, context, data }) => {
    if (context.user.id !== data.userId) {
      setResponseStatus(401)
      throw new Error('UNAUTHENTICATED')
    }
    return next({ context: { user: context.user } })
  })
```

```ts
// src/todos.functions.ts
import { createServerFn } from '@tanstack/react-start'
import { pool } from './db.server'
import { requirePageUser } from './page-user.functions'
import { realtime, todosQuery, TODOS_SQL, type Todo } from './todos.server'

// Rows for SSR plus a sealed live query for exactly the same rows.
export const loadTodos = createServerFn({ method: 'POST' })
  .middleware([requirePageUser])
  .handler(async ({ context }) => {
    const [{ rows }, query] = await Promise.all([
      pool.query<Todo>(TODOS_SQL, [context.user.id]),
      realtime.seal({ query: todosQuery(context.user.id) }),
    ])
    return { rows, query }
  })

export const refreshTodosQuery = createServerFn({ method: 'POST' })
  .middleware([requirePageUser])
  .handler(({ context }) =>
    realtime.seal({ query: todosQuery(context.user.id) }),
  )
```

- The user id in the query comes from the session (`context.user`). The
  `WHERE` clause is the authorization boundary.
- A sealed query is a bearer credential. Return it from POST server functions
  and keep pages that embed it out of shared caches.
- `refreshQuery` must seal the same SQL and parameters as the loader.
- Drizzle and Kysely queries can be sealed directly with `@neon/realtime-drizzle`
  or `@neon/realtime-kysely` passed as `adapter` to `createRealtime`.

## Client: collection descriptor

Use a descriptor so the loader and the component resolve the same collection
inside one `DbClient`:

```ts
// src/todos.collection.ts
import type { RealtimeClient, SealedLiveQuery } from '@neon/realtime/client'
import { realtimeCollectionOptions } from '@neon/realtime-tanstack'
import { collectionOptions } from '@tanstack/react-db'
import { createTodos, refreshTodosQuery } from './todos.functions'
import type { Todo } from './todos.server'

// Session gone or switched: reload, dropping this page's DbClient and rows.
function leaveIfSignedOut(error: unknown): never {
  if (error instanceof Error && error.message === 'UNAUTHENTICATED') {
    window.location.assign('/')
  }
  throw error
}

export function todosCollection(userId: string, query: SealedLiveQuery<Todo>) {
  const id = `todos:${userId}`
  return collectionOptions(id, (db) =>
    realtimeCollectionOptions({
      id,
      query,
      client: db.requireDependency<RealtimeClient>('realtime'),
      getKey: (todo) => todo.id,
      // A collection rebuilt after GC would restart from an expired sealed query.
      gcTime: 0,
      refreshQuery: () =>
        refreshTodosQuery({ data: { userId } }).catch(leaveIfSignedOut),
      onInsert: async ({ transaction, collection }) => {
        const { txid } = await createTodos({
          data: { userId, batch: transaction.mutations.map((m) => m.modified) },
        }).catch(leaveIfSignedOut)
        if (txid) await collection.utils.awaitTxId(txid, 10_000)
      },
    }),
  )
}
```

## Writes: return the txid

```ts
// src/todos.functions.ts
// Strips unknown keys, so a row's user_id never comes from the browser.
const newTodosInput = z.object({
  userId: z.string(),
  batch: z.array(
    z.object({
      id: z.string().uuid(),
      title: z.string().min(1),
      completed: z.boolean(),
    }),
  ),
})

export const createTodos = createServerFn({ method: 'POST' })
  .middleware([requirePageUser])
  .validator(newTodosInput)
  .handler(async ({ data: { batch }, context }) => {
    const { rows } = await pool.query<{ txid: string }>(
      `INSERT INTO todos (id, user_id, title, completed)
       SELECT id, $1, title, completed
       FROM jsonb_to_recordset($2::jsonb) AS t(id uuid, title text, completed boolean)
       RETURNING pg_current_xact_id()::text AS txid`,
      [context.user.id, JSON.stringify(batch)],
    )
    return { txid: rows[0]?.txid ?? null }
  })
```

- Send every mutation in the TanStack transaction in one server call, and write
  them in one SQL statement or one SQL transaction.
- Read `pg_current_xact_id()::text` inside that same transaction. The collection
  expects a decimal string.
- If no row changed, return `txid: null` and skip the wait.
- Scope `UPDATE` and `DELETE` with `user_id = <session user>` as well as the id.

## TanStack Start SSR

```tsx
// src/router.tsx
import { createRealtimeClient } from '@neon/realtime/client'
import { DbClient } from '@tanstack/react-db'
import { createRouter } from '@tanstack/react-router'
import { routerWithDbClient } from '@tanstack/react-router-with-db'
import { routeTree } from './routeTree.gen'

// routerWithDbClient requires this on the root route:
// createRootRouteWithContext<RouterContext>()
export interface RouterContext {
  readonly dbClient: DbClient
}

export function getRouter() {
  const realtime = createRealtimeClient({
    url: import.meta.env.VITE_NEON_REALTIME_URL,
  })
  const dbClient = new DbClient({ realtime })
  const router = createRouter({ routeTree, context: { dbClient } })
  return routerWithDbClient(router, dbClient)
}
```

```tsx
// src/routes/_authed/index.tsx
export const Route = createFileRoute('/_authed/')({
  staleTime: Infinity,
  loader: async ({ context }) => {
    const { rows, query } = await loadTodos({
      data: { userId: context.user.id },
    })
    context.dbClient.collection(todosCollection(context.user.id, query), {
      initialData: rows,
    })
    return { query }
  },
  component: TodosPage,
})

function TodosPage() {
  const { query } = Route.useLoaderData()
  const { user } = Route.useRouteContext()
  const todos = todosCollection(user.id, query)
  const { data, isReady } = useLiveQuery({
    query: (q) => q.from({ todo: todos }).orderBy(({ todo }) => todo.title),
  })
  // ...
}
```

The server renders the seeded rows without subscribing. `routerWithDbClient`
restores them in the browser before hydration, then the collection subscribes
with the sealed query and the first live result replaces the seeded rows. Set
`Cache-Control: private, no-store` and `Vary: Cookie` on routes that embed
rows or sealed queries.

## Common Mistakes

### CRITICAL Taking the user id from the client

Sealing `todosQuery(data.userId)` lets any caller subscribe to another user's
rows. Seal with the session user from middleware.

### CRITICAL Sharing a DbClient between requests

A module-global `DbClient` or collection leaks one user's rows into another
user's SSR response. Create the `DbClient` in `getRouter()`.

### HIGH Treating an awaitTxId timeout as a failed write

Once the server returned a txid, the write committed. A timeout means the live
query has not delivered it yet (for example during a reconnect). Show the
change as saved; do not resend it.

### HIGH Sealing a different query in refreshQuery

`refreshQuery` replaces the subscription in place. A different query or
parameters swaps the collection's contents.

### MEDIUM ORDER BY in the live query

The collection is keyed and unordered, so server order is dropped. Sort with
`orderBy` in `useLiveQuery`.

### MEDIUM Column names that differ from row keys

Realtime matches result columns to keys by name. Alias snake_case columns
(`created_at AS "createdAt"`) when the row type uses camelCase.

## Sources

- https://neon.com/docs/tanstack/realtime-db.md
- https://neon.com/docs/realtime/examples/tanstack
- https://neon.com/docs/realtime/sdks/typescript#tanstack-db
- https://neon.com/docs/realtime/guides/queries
- https://neon.com/docs/realtime/guides/mutations
- https://neon.com/docs/realtime/guides/ssr
