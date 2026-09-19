use rand::Rng;
use sha2::{Sha256, Digest};

pub struct Authenticator {
    passphrase: String,
}

impl Authenticator {
    pub fn new(passphrase: String) -> Self {
        Self { passphrase }
    }

    pub fn generate_challenge(&self) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut challenge = vec![0u8; 32];
        rng.fill(&mut challenge[..]);
        challenge
    }

    pub fn generate_response(&self, challenge: &[u8]) -> Vec<u8> {
        // Combine passphrase with challenge
        let mut hasher = Sha256::new();
        hasher.update(self.passphrase.as_bytes());
        hasher.update(challenge);
        hasher.finalize().to_vec()
    }

    pub fn verify_response(&self, challenge: &[u8], response: &[u8]) -> bool {
        let expected = self.generate_response(challenge);
        expected == response
    }

    pub fn derive_shared_secret(&self) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(self.passphrase.as_bytes());
        hasher.finalize().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authenticator_creation() {
        let auth = Authenticator::new("test_passphrase".to_string());
        assert_eq!(auth.passphrase, "test_passphrase");
    }

    #[test]
    fn test_generate_challenge_length() {
        let auth = Authenticator::new("test".to_string());
        let challenge = auth.generate_challenge();
        assert_eq!(challenge.len(), 32);
    }

    #[test]
    fn test_generate_challenge_randomness() {
        let auth = Authenticator::new("test".to_string());
        let challenge1 = auth.generate_challenge();
        let challenge2 = auth.generate_challenge();
        assert_ne!(challenge1, challenge2);
    }

    #[test]
    fn test_generate_response_deterministic() {
        let auth = Authenticator::new("test_passphrase".to_string());
        let challenge = vec![1, 2, 3, 4, 5];

        let response1 = auth.generate_response(&challenge);
        let response2 = auth.generate_response(&challenge);

        assert_eq!(response1, response2);
        assert_eq!(response1.len(), 32);
    }

    #[test]
    fn test_verify_response_valid() {
        let auth = Authenticator::new("secret_passphrase".to_string());
        let challenge = auth.generate_challenge();
        let response = auth.generate_response(&challenge);

        assert!(auth.verify_response(&challenge, &response));
    }

    #[test]
    fn test_verify_response_invalid_response() {
        let auth = Authenticator::new("secret_passphrase".to_string());
        let challenge = auth.generate_challenge();
        let wrong_response = vec![0u8; 32];

        assert!(!auth.verify_response(&challenge, &wrong_response));
    }

    #[test]
    fn test_verify_response_wrong_passphrase() {
        let auth1 = Authenticator::new("passphrase1".to_string());
        let auth2 = Authenticator::new("passphrase2".to_string());

        let challenge = auth1.generate_challenge();
        let response = auth1.generate_response(&challenge);

        assert!(!auth2.verify_response(&challenge, &response));
    }

    #[test]
    fn test_verify_response_tampered_challenge() {
        let auth = Authenticator::new("secret_passphrase".to_string());
        let challenge = auth.generate_challenge();
        let response = auth.generate_response(&challenge);

        let mut tampered_challenge = challenge.clone();
        tampered_challenge[0] ^= 0xFF;

        assert!(!auth.verify_response(&tampered_challenge, &response));
    }

    #[test]
    fn test_derive_shared_secret_deterministic() {
        let auth = Authenticator::new("shared_secret_phrase".to_string());

        let secret1 = auth.derive_shared_secret();
        let secret2 = auth.derive_shared_secret();

        assert_eq!(secret1, secret2);
        assert_eq!(secret1.len(), 32);
    }

    #[test]
    fn test_derive_shared_secret_different_passphrase() {
        let auth1 = Authenticator::new("passphrase1".to_string());
        let auth2 = Authenticator::new("passphrase2".to_string());

        let secret1 = auth1.derive_shared_secret();
        let secret2 = auth2.derive_shared_secret();

        assert_ne!(secret1, secret2);
    }
}
