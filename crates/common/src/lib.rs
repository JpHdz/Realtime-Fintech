pub mod trades {
        include!(concat!(env!("OUT_DIR"), "/common.rs"));
}

pub use trades::Trade;