//use lettre::sendmail::SendmailTransport;
//use lettre::Transport;
use crate::*;
use lettre_email::Email;
use std::fs::OpenOptions;
use std::io::Write;

pub(crate) fn send(
    _log: slog::Logger,
    sender: String,
    recipients: Vec<String>,
    subject: String,
    text: String,
) -> Result<(), lettre::sendmail::error::Error> {
    //let mut mailer = SendmailTransport::new();

    let mut builder = Email::builder()
        .from(sender.clone())
        .subject(subject.clone())
        .text(text.clone());
    for recipient in &recipients {
        builder = builder.to(recipient.to_string());
    }

    /*let email = builder.build();
    match email {
        Ok(result) => mailer.send(result.into())?,
        Err(e) => {
            println!("couldn't construct email: {}", e);
        }
    }*/

    // XXX for testing
    let parts = text.split("\n");
    for part in parts {
        let subparts: Vec<&str> = part.split("#").collect();
        let filename: String;
        match subparts[0].trim() {
            "APIKEY" => {
                filename = format!("{}.{}", recipients[0], APIKEY_FILE);
            }
            "SHARE" => {
                filename = format!("{}.{}", recipients[0], SHARE_FILE);
            }
            "DID" => {
                filename = format!("{}.{}", recipients[0], DID_FILE);
            }
            _ => return Ok(()),
        };
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(filename)
            .unwrap();
        if let Err(e) = write!(f, "{}", format!("{}", subparts[1].trim())) {
            eprintln!("Couldn't write to file: {}", e);
        }
    }
    Ok(())
}
