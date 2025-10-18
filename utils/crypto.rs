use age::{
    armor::{ArmoredReader, ArmoredWriter, Format},
    secrecy::Secret,
    Decryptor, Encryptor,
};
use std::io::Read;

pub fn get_passphrase() -> eyre::Result<String> {
    std::env::var("ENCRYPTION_PASSPHRASE")
        .or_else(|_| {
            println!("Warning: ENCRYPTION_PASSPHRASE environment variable not set, using default 'polymarket'");
            Ok("polymarket".to_string())
        })
}

pub fn encrypt<R: Read>(mut data: R, passphrase: &str) -> eyre::Result<Vec<u8>> {
    let encryptor = Encryptor::with_user_passphrase(Secret::new(passphrase.to_string()));
    let mut encrypted = vec![];

    {
        let mut writer = ArmoredWriter::wrap_output(&mut encrypted, Format::AsciiArmor)?;
        let mut output = encryptor.wrap_output(&mut writer)?;
        std::io::copy(&mut data, &mut output)?;
        output.finish()?;
        writer.finish()?;
    }

    Ok(encrypted)
}

pub fn decrypt<R: Read>(encrypted_data: R, passphrase: &str) -> eyre::Result<String> {
    let armored_reader = ArmoredReader::new(encrypted_data);
    let decryptor = match Decryptor::new(armored_reader)? {
        Decryptor::Passphrase(d) => d,
        _ => eyre::bail!("Expected passphrase decryptor"),
    };

    let mut decrypted = vec![];
    let mut reader = decryptor.decrypt(&Secret::new(passphrase.to_string()), None)?;
    reader.read_to_end(&mut decrypted)?;

    Ok(String::from_utf8(decrypted)?)
}
