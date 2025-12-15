// packages/plugin-clickhouse/src/utilities/actions.ts
import type { Payload } from 'payload'

export interface EnqueueArgs {
  assignedTo?: string
  collection?: string
  docId?: string
  input?: Record<string, unknown>
  maxAttempts?: number
  name: string
  parentId?: string
  priority?: number
  rootId?: string
  scheduledAt?: Date
  steps?: Array<{
    handler: string
    input?: Record<string, unknown>
    name: string
    waitingFor?: 'approval' | 'external' | 'input' | 'review'
  }>
  timeoutMs?: number
  type?: 'job' | 'task' | 'transaction' | 'workflow'
  waitingFor?: 'approval' | 'external' | 'input' | 'review'
}

export interface ActionRecord {
  assignedTo: null | string
  attempts: number
  collection: null | string
  completedAt: null | string
  context: Record<string, unknown>
  createdAt: string
  docId: null | string
  error: null | Record<string, unknown>
  id: string
  input: Record<string, unknown>
  maxAttempts: number
  name: string
  output: Record<string, unknown>
  parentId: null | string
  priority: number
  retryAfter: null | string
  rootId: null | string
  scheduledAt: null | string
  startedAt: null | string
  status: string
  step: number
  steps: unknown[]
  timeoutAt: null | string
  type: string
  updatedAt: string
  waitingFor: null | string
}

export interface ClaimActionsArgs {
  limit?: number
  lockFor?: number // milliseconds
  name?: string
  type?: string
}

export interface CompleteActionArgs {
  id: string
  output?: Record<string, unknown>
}

export interface FailActionArgs {
  error: Record<string, unknown>
  id: string
  retryAfter?: Date
}

export interface ResumeActionArgs {
  id: string
  input?: Record<string, unknown>
}

/**
 * Enqueue a new action/job
 */
export const createEnqueue = (payload: Payload) => {
  return async (args: EnqueueArgs): Promise<string> => {
    if (typeof payload.db.execute !== 'function') {
      throw new Error('execute not available on database adapter')
    }

    const id =
      payload.db.idType === 'uuid' ? crypto.randomUUID() : (await import('nanoid')).nanoid()

    const now = Date.now()
    const timeoutAt = args.timeoutMs ? new Date(now + args.timeoutMs) : null

    await payload.db.execute({
      query: `
        INSERT INTO actions (
          id, ns, type, name, status, priority,
          collection, docId, input, output, error,
          step, steps, context,
          assignedTo, waitingFor,
          scheduledAt, startedAt, completedAt, timeoutAt,
          attempts, maxAttempts, retryAfter,
          parentId, rootId,
          createdAt, updatedAt, v
        ) VALUES (
          {id:String}, {ns:String}, {type:String}, {name:String}, {status:String}, {priority:Int32},
          {collection:Nullable(String)}, {docId:Nullable(String)}, {input:String}, {output:String}, {error:Nullable(String)},
          {step:Int32}, {steps:String}, {context:String},
          {assignedTo:Nullable(String)}, {waitingFor:Nullable(String)},
          {scheduledAt:Nullable(DateTime64(3))}, {startedAt:Nullable(DateTime64(3))}, {completedAt:Nullable(DateTime64(3))}, {timeoutAt:Nullable(DateTime64(3))},
          {attempts:Int32}, {maxAttempts:Int32}, {retryAfter:Nullable(DateTime64(3))},
          {parentId:Nullable(String)}, {rootId:Nullable(String)},
          {createdAt:DateTime64(3)}, {updatedAt:DateTime64(3)}, {v:DateTime64(3)}
        )
      `,
      query_params: {
        id,
        name: args.name,
        type: args.type || 'job',
        assignedTo: args.assignedTo || null,
        attempts: 0,
        collection: args.collection || null,
        completedAt: null,
        context: JSON.stringify({}),
        createdAt: now,
        docId: args.docId || null,
        error: null,
        input: JSON.stringify(args.input || {}),
        maxAttempts: args.maxAttempts || 3,
        ns: payload.db.namespace,
        output: JSON.stringify({}),
        parentId: args.parentId || null,
        priority: args.priority || 0,
        retryAfter: null,
        rootId: args.rootId || null,
        scheduledAt: args.scheduledAt || null,
        startedAt: null,
        status: args.scheduledAt ? 'pending' : 'pending',
        step: 0,
        steps: JSON.stringify(args.steps || []),
        timeoutAt,
        updatedAt: now,
        v: now,
        waitingFor: args.waitingFor || null,
      },
    })

    return id
  }
}

/**
 * Enqueue multiple actions in a batch
 */
export const createEnqueueBatch = (payload: Payload) => {
  const enqueue = createEnqueue(payload)

  return async (actions: EnqueueArgs[]): Promise<string[]> => {
    const ids: string[] = []
    for (const action of actions) {
      const id = await enqueue(action)
      ids.push(id)
    }
    return ids
  }
}

/**
 * Claim actions for processing (atomic operation)
 */
export const createClaimActions = (payload: Payload) => {
  return async ({
    name,
    type,
    limit = 10,
    lockFor = 60000,
  }: ClaimActionsArgs): Promise<ActionRecord[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const now = Date.now()
    const lockUntil = new Date(now + lockFor)

    // Find and claim pending actions
    const nameFilter = name ? 'AND name = {name:String}' : ''
    const typeFilter = type ? 'AND type = {type:String}' : ''

    const actions = await payload.db.execute<ActionRecord>({
      query: `
        SELECT *
        FROM actions
        WHERE ns = {ns:String}
          AND status = 'pending'
          AND (scheduledAt IS NULL OR scheduledAt <= {now:DateTime64(3)})
          AND (retryAfter IS NULL OR retryAfter <= {now:DateTime64(3)})
          ${nameFilter}
          ${typeFilter}
        ORDER BY priority DESC, createdAt ASC
        LIMIT {limit:UInt32}
      `,
      query_params: {
        name: name || '',
        type: type || '',
        limit,
        now,
        ns: payload.db.namespace,
      },
    })

    if (actions.length === 0) {
      return []
    }

    // Mark as running
    const ids = actions.map((a) => a.id)
    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE status = 'running', startedAt = {now:DateTime64(3)}, timeoutAt = {lockUntil:DateTime64(3)}, attempts = attempts + 1, v = {now:DateTime64(3)}
        WHERE id IN ({ids:Array(String)}) AND ns = {ns:String}
      `,
      query_params: {
        ids,
        lockUntil,
        now,
        ns: payload.db.namespace,
      },
    })

    return actions
  }
}

/**
 * Complete an action successfully
 */
export const createCompleteAction = (payload: Payload) => {
  return async ({ id, output = {} }: CompleteActionArgs): Promise<void> => {
    if (typeof payload.db.execute !== 'function') {
      return
    }

    const now = Date.now()
    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE status = 'completed', output = {output:String}, completedAt = {now:DateTime64(3)}, v = {now:DateTime64(3)}
        WHERE id = {id:String} AND ns = {ns:String}
      `,
      query_params: {
        id,
        now,
        ns: payload.db.namespace,
        output: JSON.stringify(output),
      },
    })
  }
}

/**
 * Fail an action (will retry if attempts < maxAttempts)
 */
export const createFailAction = (payload: Payload) => {
  return async ({ id, error, retryAfter }: FailActionArgs): Promise<void> => {
    if (typeof payload.db.execute !== 'function') {
      return
    }

    const now = Date.now()

    // Check if should retry
    const [action] = await payload.db.execute<ActionRecord>({
      query: `SELECT attempts, maxAttempts FROM actions WHERE id = {id:String} AND ns = {ns:String}`,
      query_params: { id, ns: payload.db.namespace },
    })

    const shouldRetry = action && action.attempts < action.maxAttempts
    const newStatus = shouldRetry ? 'pending' : 'failed'

    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE
          status = {status:String},
          error = {error:String},
          retryAfter = {retryAfter:Nullable(DateTime64(3))},
          completedAt = {completedAt:Nullable(DateTime64(3))},
          v = {now:DateTime64(3)}
        WHERE id = {id:String} AND ns = {ns:String}
      `,
      query_params: {
        id,
        completedAt: shouldRetry ? null : now,
        error: JSON.stringify(error),
        now,
        ns: payload.db.namespace,
        retryAfter: shouldRetry ? retryAfter || new Date(now + 60000) : null,
        status: newStatus,
      },
    })
  }
}

/**
 * Cancel an action
 */
export const createCancelAction = (payload: Payload) => {
  return async ({ id }: { id: string }): Promise<void> => {
    if (typeof payload.db.execute !== 'function') {
      return
    }

    const now = Date.now()
    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE status = 'cancelled', completedAt = {now:DateTime64(3)}, v = {now:DateTime64(3)}
        WHERE id = {id:String} AND ns = {ns:String}
      `,
      query_params: { id, now, ns: payload.db.namespace },
    })
  }
}

/**
 * Resume an action that was waiting for input
 */
export const createResumeAction = (payload: Payload) => {
  return async ({ id, input = {} }: ResumeActionArgs): Promise<void> => {
    if (typeof payload.db.execute !== 'function') {
      return
    }

    const now = Date.now()

    // Get current action to merge context
    const [action] = await payload.db.execute<ActionRecord>({
      query: `SELECT context, step FROM actions WHERE id = {id:String} AND ns = {ns:String}`,
      query_params: { id, ns: payload.db.namespace },
    })

    const currentContext = action ? JSON.parse(String(action.context)) : {}
    const newContext = {
      ...currentContext,
      [`step_${action?.step}_input`]: input,
    }

    await payload.db.execute({
      query: `
        ALTER TABLE actions
        UPDATE
          status = 'pending',
          waitingFor = NULL,
          context = {context:String},
          step = step + 1,
          v = {now:DateTime64(3)}
        WHERE id = {id:String} AND ns = {ns:String}
      `,
      query_params: {
        id,
        context: JSON.stringify(newContext),
        now,
        ns: payload.db.namespace,
      },
    })
  }
}

/**
 * Get actions for a specific document
 */
export const createGetDocumentActions = (payload: Payload) => {
  return async ({
    collection,
    docId,
    status,
  }: {
    collection: string
    docId: string
    status?: string[]
  }): Promise<ActionRecord[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const statusFilter = status?.length ? 'AND status IN ({status:Array(String)})' : ''

    return await payload.db.execute<ActionRecord>({
      query: `
        SELECT *
        FROM actions
        WHERE collection = {collection:String}
          AND docId = {docId:String}
          AND ns = {ns:String}
          ${statusFilter}
        ORDER BY createdAt DESC
      `,
      query_params: {
        collection,
        docId,
        ns: payload.db.namespace,
        status: status || [],
      },
    })
  }
}

/**
 * Get tasks assigned to a user
 */
export const createGetAssignedTasks = (payload: Payload) => {
  return async ({
    userId,
    waitingFor,
  }: {
    userId: string
    waitingFor?: string
  }): Promise<ActionRecord[]> => {
    if (typeof payload.db.execute !== 'function') {
      return []
    }

    const waitingForFilter = waitingFor ? 'AND waitingFor = {waitingFor:String}' : ''

    return await payload.db.execute<ActionRecord>({
      query: `
        SELECT *
        FROM actions
        WHERE assignedTo = {userId:String}
          AND status = 'waiting'
          AND ns = {ns:String}
          ${waitingForFilter}
        ORDER BY priority DESC, createdAt ASC
      `,
      query_params: {
        ns: payload.db.namespace,
        userId,
        waitingFor: waitingFor || '',
      },
    })
  }
}
