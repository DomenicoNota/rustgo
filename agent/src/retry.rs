use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter_ratio: f64,
}

impl RetryPolicy {
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        self.delay_for_attempt_with_sample(attempt, fastrand::f64())
    }

    pub fn delay_for_attempt_with_sample(&self, attempt: usize, sample: f64) -> Duration {
        let exponent = attempt.saturating_sub(1).min(31) as u32;
        let multiplier = 1_u128 << exponent;
        let uncapped_ms = self.base_delay.as_millis().saturating_mul(multiplier);
        let capped_ms = uncapped_ms.min(self.max_delay.as_millis());
        let centered_sample = sample.clamp(0.0, 1.0) * 2.0 - 1.0;
        let jittered_ms = (capped_ms as f64 * (1.0 + centered_sample * self.jitter_ratio))
            .round()
            .clamp(0.0, self.max_delay.as_millis() as f64) as u64;
        Duration::from_millis(jittered_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 8,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            jitter_ratio: 0.2,
        }
    }

    #[test]
    fn exponential_backoff_is_capped() {
        let policy = policy();
        assert_eq!(
            policy.delay_for_attempt_with_sample(1, 0.5),
            Duration::from_millis(100)
        );
        assert_eq!(
            policy.delay_for_attempt_with_sample(3, 0.5),
            Duration::from_millis(400)
        );
        assert_eq!(
            policy.delay_for_attempt_with_sample(8, 0.5),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn jitter_stays_within_policy_bounds() {
        let policy = policy();
        assert_eq!(
            policy.delay_for_attempt_with_sample(1, 0.0),
            Duration::from_millis(80)
        );
        assert_eq!(
            policy.delay_for_attempt_with_sample(1, 1.0),
            Duration::from_millis(120)
        );
        assert_eq!(
            policy.delay_for_attempt_with_sample(20, 1.0),
            policy.max_delay
        );
    }
}
