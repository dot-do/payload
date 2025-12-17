/**
 * Test configuration for integration tests
 */

import type { Config, SanitizedConfig } from 'payload'

import { buildConfig } from 'payload'

/**
 * Create a minimal Payload config for testing
 */
export function createTestConfig(dbConfig: Config['db']): Promise<SanitizedConfig> {
  return buildConfig({
    collections: [
      {
        slug: 'posts',
        fields: [
          {
            name: 'title',
            type: 'text',
            required: true,
          },
          {
            name: 'content',
            type: 'textarea',
          },
          {
            name: 'status',
            type: 'select',
            defaultValue: 'draft',
            options: ['draft', 'published'],
          },
        ],
      },
      {
        slug: 'users',
        auth: true,
        fields: [
          {
            name: 'name',
            type: 'text',
          },
        ],
      },
    ],
    db: dbConfig,
    secret: 'test-secret-key-for-jwt-signing-min-32-chars',
  })
}
