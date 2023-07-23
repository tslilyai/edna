use chrono::Local;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum GenValue {
    ConstNum(u64),
    ConstStr(String),
    RandNum { lb: usize, ub: usize },
    RandStr { len: usize },
    RandEmail,
    RandPhone,
    Timestamp,
    Bool(bool),
    Null,
}

pub fn gen_strval(v: &GenValue) -> String {
    use GenValue::*;
    match v {
        ConstNum(n) => n.to_string(),
        ConstStr(s) => format!("\'{}\'", s.to_string()),
        RandNum { lb, ub } => {
            let mut rng = rand::thread_rng();
            rng.gen_range(*lb..*ub).to_string()
        }
        RandStr { len } => {
            let rng = rand::thread_rng();
            let rand_string: String = rng
                .sample_iter(&Alphanumeric)
                .take(*len)
                .map(char::from)
                .collect();
            format!("\'{}\'", rand_string)
        }
        RandEmail => {
            let rng = rand::thread_rng();
            let rand_string: String = rng
                .sample_iter(&Alphanumeric)
                .take(20)
                .map(char::from)
                .collect();
            format!("\'{}@anon.com\'", rand_string)
        }
        RandPhone => {
            let mut rng = rand::thread_rng();
            const CHARSET: &[u8] = b"0123456789";
            const LEN: usize = 9;
            let rand_phone: String = (0..LEN)
                .map(|_| {
                    let idx = rng.gen_range(0..CHARSET.len());
                    CHARSET[idx] as char
                })
                .collect();
            format!("\'{}\'", rand_phone)
        }
        Bool(b) => b.to_string(),
        Timestamp => {
            format!("\'{}\'", Local::now().naive_local().to_string())
        }
        Null => "NULL".to_string(),
    }
}
