import { MykoLogger } from '@myko/core'

const logger = new MykoLogger('SubscriptionTracker')

export class SubscriptionTracker {
  private static subscriptionCounts = new Map<string, number>()
  private static enabled = process.env['DEBUG_SUBSCRIPTIONS'] === 'true'

  static track(type: string, id: string, action: 'subscribe' | 'unsubscribe') {
    if (!this.enabled) return

    const key = `${type}:${id}`
    const current = this.subscriptionCounts.get(key) || 0
    const newCount =
      action === 'subscribe' ? current + 1 : Math.max(0, current - 1)

    this.subscriptionCounts.set(key, newCount)

    if (newCount > 10) {
      logger.warn(`High subscription count for ${key}: ${newCount}`)
    }
  }

  static report() {
    if (!this.enabled) return

    const total = Array.from(this.subscriptionCounts.values()).reduce(
      (a, b) => a + b,
      0,
    )
    const byType = new Map<string, number>()

    for (const [key, count] of this.subscriptionCounts.entries()) {
      const type = key.split(':')[0]
      byType.set(type, (byType.get(type) || 0) + count)
    }

    logger.info('Subscription Report:')
    logger.info(`Total active subscriptions: ${total}`)
    for (const [type, count] of byType.entries()) {
      logger.info(`  ${type}: ${count}`)
    }
  }
}

// Report every 30 seconds if enabled
if (SubscriptionTracker['enabled']) {
  setInterval(() => SubscriptionTracker.report(), 30000)
}
