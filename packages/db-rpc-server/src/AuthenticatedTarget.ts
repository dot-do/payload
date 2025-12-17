/**
 * AuthenticatedDatabaseTarget
 *
 * RpcTarget implementation that provides authenticated database operations.
 * All operations include the authenticated user in the request context.
 */

import type { FindDistinctArgs } from '@payloadcms/db-rpc'
import type {
  BaseDatabaseAdapter,
  CountArgs,
  CountGlobalVersionArgs,
  CreateArgs,
  CreateGlobalArgs,
  CreateGlobalVersionArgs,
  CreateVersionArgs,
  DeleteManyArgs,
  DeleteOneArgs,
  DeleteVersionsArgs,
  FindArgs,
  FindGlobalArgs,
  FindGlobalVersionsArgs,
  FindOneArgs,
  FindVersionsArgs,
  JsonObject,
  Payload,
  PayloadRequest,
  QueryDraftsArgs,
  TypedUser,
  TypeWithID,
  UpdateGlobalArgs,
  UpdateGlobalVersionArgs,
  UpdateJobsArgs,
  UpdateManyArgs,
  UpdateOneArgs,
  UpdateVersionArgs,
  UpsertArgs,
} from 'payload'

import { RpcTarget } from 'capnweb'

/**
 * Arguments with optional transaction ID
 */
type WithTransaction<T> = { transactionID?: string } & T

/**
 * Authenticated database target that wraps a database adapter
 *
 * Note: We don't use `implements AuthenticatedDatabaseApi` because TypeScript's
 * strict generic checking causes issues with payload's union types (e.g., UpdateOneArgs).
 * The implementation is correct and matches the interface at runtime.
 */
export class AuthenticatedDatabaseTarget extends RpcTarget {
  #adapter: BaseDatabaseAdapter
  #payload: Payload
  #user: TypedUser

  constructor(adapter: BaseDatabaseAdapter, payload: Payload, user: TypedUser) {
    super()
    this.#adapter = adapter
    this.#payload = payload
    this.#user = user
  }

  /**
   * Build a partial PayloadRequest with user and transaction context
   */
  #buildReq(transactionID?: string): Partial<PayloadRequest> {
    return {
      payload: this.#payload,
      transactionID,
      user: this.#user,
    }
  }

  // ============== Transaction Methods ==============

  async beginTransaction(options?: Record<string, unknown>): Promise<null | string> {
    const result = await this.#adapter.beginTransaction(options)
    return result !== null ? String(result) : null
  }

  async commitTransaction(txId: string): Promise<void> {
    await this.#adapter.commitTransaction(txId)
  }

  async count(args: WithTransaction<Omit<CountArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.count({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  // ============== Collection CRUD ==============

  async countGlobalVersions(args: WithTransaction<Omit<CountGlobalVersionArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.countGlobalVersions({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async countVersions(args: WithTransaction<Omit<CountArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.countVersions({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async create(args: WithTransaction<Omit<CreateArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.create({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async createGlobal<T extends Record<string, unknown>>(
    args: WithTransaction<Omit<CreateGlobalArgs<T>, 'req'>>,
  ) {
    const { transactionID, ...rest } = args
    return this.#adapter.createGlobal<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async createGlobalVersion<T extends JsonObject>(
    args: WithTransaction<Omit<CreateGlobalVersionArgs<T>, 'req'>>,
  ) {
    const { transactionID, ...rest } = args
    return this.#adapter.createGlobalVersion<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async createVersion<T extends JsonObject>(
    args: WithTransaction<Omit<CreateVersionArgs<T>, 'req'>>,
  ) {
    const { transactionID, ...rest } = args
    return this.#adapter.createVersion<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async deleteMany(args: WithTransaction<Omit<DeleteManyArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    await this.#adapter.deleteMany({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async deleteOne(args: WithTransaction<Omit<DeleteOneArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.deleteOne({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async deleteVersions(args: WithTransaction<Omit<DeleteVersionsArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    await this.#adapter.deleteVersions({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async find<T>(args: WithTransaction<Omit<FindArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.find<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async findDistinct(args: WithTransaction<Omit<FindDistinctArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    // Type assertion needed because FindDistinctArgs has required fields
     
    return this.#adapter.findDistinct({
      ...rest,
      req: this.#buildReq(transactionID),
    } as any)
  }

  // ============== Globals ==============

  async findGlobal<T extends Record<string, unknown>>(
    args: WithTransaction<Omit<FindGlobalArgs, 'req'>>,
  ) {
    const { transactionID, ...rest } = args
    return this.#adapter.findGlobal<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async findGlobalVersions<T extends JsonObject>(
    args: WithTransaction<Omit<FindGlobalVersionsArgs, 'req'>>,
  ) {
    const { transactionID, ...rest } = args
    return this.#adapter.findGlobalVersions<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async findOne<T extends TypeWithID>(args: WithTransaction<Omit<FindOneArgs, 'req'>>) {
    const { transactionID, ...rest } = args
     
    return this.#adapter.findOne<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    } as any)
  }

  // ============== Versions ==============

  async findVersions<T extends JsonObject>(args: WithTransaction<Omit<FindVersionsArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.findVersions<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async queryDrafts<T>(args: WithTransaction<Omit<QueryDraftsArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.queryDrafts<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async rollbackTransaction(txId: string): Promise<void> {
    await this.#adapter.rollbackTransaction(txId)
  }

  async updateGlobal<T extends Record<string, unknown>>(
    args: WithTransaction<Omit<UpdateGlobalArgs<T>, 'req'>>,
  ) {
    const { transactionID, ...rest } = args
    return this.#adapter.updateGlobal<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async updateGlobalVersion<T extends JsonObject>(
    args: WithTransaction<Omit<UpdateGlobalVersionArgs<T>, 'req'>>,
  ) {
    const { transactionID, ...rest } = args
     
    return this.#adapter.updateGlobalVersion<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    } as any)
  }

  // ============== Global Versions ==============

  async updateJobs(args: WithTransaction<Omit<UpdateJobsArgs, 'req'>>) {
    const { transactionID, ...rest } = args
     
    return this.#adapter.updateJobs({
      ...rest,
      req: this.#buildReq(transactionID),
    } as any)
  }

  async updateMany(args: WithTransaction<Omit<UpdateManyArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.updateMany({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }

  async updateOne(args: WithTransaction<Omit<UpdateOneArgs, 'req'>>) {
    const { transactionID, ...rest } = args
     
    return this.#adapter.updateOne({
      ...rest,
      req: this.#buildReq(transactionID),
    } as any)
  }

  async updateVersion<T extends JsonObject>(
    args: WithTransaction<Omit<UpdateVersionArgs<T>, 'req'>>,
  ) {
    const { transactionID, ...rest } = args
     
    return this.#adapter.updateVersion<T>({
      ...rest,
      req: this.#buildReq(transactionID),
    } as any)
  }

  // ============== Jobs ==============

  async upsert(args: WithTransaction<Omit<UpsertArgs, 'req'>>) {
    const { transactionID, ...rest } = args
    return this.#adapter.upsert({
      ...rest,
      req: this.#buildReq(transactionID),
    })
  }
}
