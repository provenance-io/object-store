// TODO look at https://docs.rs/aes-gcm/0.8.0/aes_gcm/ for production flags for acceleration
// TODO fix all unwraps
// TODO add additional data
use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, NewAead, generic_array::GenericArray};
use bytes::{Bytes, BytesMut, BufMut};
use rand::{rngs::StdRng, RngCore, SeedableRng};

const NONCE_BYTES: usize = 12;
const KEY_BYTES: usize = 32;

trait Crypto {
    fn nonce(&mut self) -> Vec<u8>;
    fn key(&mut self) -> Vec<u8>;
}

struct CryptoRandom {
    rng: rand::rngs::StdRng,
}

impl Default for CryptoRandom {
    fn default() -> Self {
        Self { rng: StdRng::from_entropy() }
    }
}

impl Crypto for CryptoRandom {
    fn nonce(&mut self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(NONCE_BYTES);
        // TODO use try fill instead?
        self.rng.fill_bytes(buffer.as_mut_slice());

        buffer
    }

    fn key(&mut self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(KEY_BYTES);
        // TODO use try fill instead?
        self.rng.fill_bytes(buffer.as_mut_slice());

        buffer
    }
}

struct CryptoTest;

impl Crypto for CryptoTest {
    fn nonce(&mut self) -> Vec<u8> {
        vec![0_u8; 12]
    }

    fn key(&mut self) -> Vec<u8> {
        b"an example very very secret key.".to_vec()
    }
}

fn encrypt(input: &[u8], crypto: &mut impl Crypto) -> Bytes {
    let nonce_vec = crypto.nonce();
    let nonce = GenericArray::from_slice(nonce_vec.as_ref());
    let key = crypto.key();
    let key = GenericArray::from_slice(key.as_ref());

    let cipher = Aes256Gcm::new(key);
    let cipher_text = cipher.encrypt(nonce, input).unwrap();
    let mut buffer = BytesMut::with_capacity(4 + NONCE_BYTES + cipher_text.len());

    buffer.put_u32(NONCE_BYTES as u32);
    buffer.put_slice(nonce_vec.as_ref());
    buffer.put_slice(cipher_text.as_ref());

    buffer.into()
}

#[cfg(test)]
mod tests {
    use crate::dime::aes::*;

    #[test]
    fn simple() {
        let mut rng = CryptoTest;
        let payload = b"testing this payload";

        let cipher_text = encrypt(payload, &mut rng);

        assert_eq!(hex::encode(&cipher_text), "0000000c00000000000000000000000076d8a74c5f8bac7ef7e92a7e38a4570560d49e23f69a4bb3d94517715bfd8189d42161f9");
    }
}
