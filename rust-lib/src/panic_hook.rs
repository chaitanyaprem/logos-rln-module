use std::panic;
use std::sync::Once;

static INSTALL: Once = Once::new();

pub(crate) fn install_once() {
    INSTALL.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            let payload = info.payload();
            let msg = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");
            match info.location() {
                Some(loc) => eprintln!(
                    "membership panic at {}:{}:{}: {msg}",
                    loc.file(),
                    loc.line(),
                    loc.column()
                ),
                None => eprintln!("membership panic at <unknown location>: {msg}"),
            }
            previous(info);
        }));
    });
}
