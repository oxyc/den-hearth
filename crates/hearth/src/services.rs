//! Which services this process starts. The release image contains all four, but a small self-hosted
//! node should not have to initialise media workers or expose routes it does not use.

use crate::route::Service;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Services {
    scout: bool,
    reel: bool,
    subs: bool,
    remux: bool,
}

impl Services {
    pub const ALL: Self = Self { scout: true, reel: true, subs: true, remux: true };

    pub fn parse(raw: Option<String>) -> Result<Self, String> {
        let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
            return Ok(Self::ALL);
        };
        if raw.trim().eq_ignore_ascii_case("all") {
            return Ok(Self::ALL);
        }
        let mut services = Self { scout: false, reel: false, subs: false, remux: false };
        for item in raw.split(',').map(str::trim).filter(|item| !item.is_empty()) {
            match item.to_ascii_lowercase().as_str() {
                "scout" => services.scout = true,
                "reel" => services.reel = true,
                "subs" | "subtitles" => services.subs = true,
                "remux" => services.remux = true,
                _ => {
                    return Err(format!(
                        "HEARTH_SERVICES contains {item:?}; expected scout, reel, subs, remux, or all"
                    ));
                }
            }
        }
        if !services.any() {
            return Err("HEARTH_SERVICES enables no services".to_string());
        }
        Ok(services)
    }

    pub fn has(self, service: Service) -> bool {
        match service {
            Service::Scout => self.scout,
            Service::Reel => self.reel,
            Service::Subs => self.subs,
            Service::Remux => self.remux,
        }
    }

    pub fn any(self) -> bool {
        self.scout || self.reel || self.subs || self.remux
    }

    /// The combined Den addon is made from these three services. Partial selections remain available
    /// through their individual mounts instead of advertising a combined manifest with dead resources.
    pub fn den(self) -> bool {
        self.scout && self.reel && self.subs
    }

    pub fn playback(self) -> bool {
        self.den() && self.remux
    }

    pub fn names(self) -> Vec<&'static str> {
        [Service::Scout, Service::Reel, Service::Subs, Service::Remux]
            .into_iter()
            .filter(|service| self.has(*service))
            .map(crate::ports::name)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_and_all_keep_the_compatible_default() {
        assert_eq!(Services::parse(None).unwrap(), Services::ALL);
        assert_eq!(Services::parse(Some("all".into())).unwrap(), Services::ALL);
    }

    #[test]
    fn a_subset_accepts_the_human_subtitles_alias() {
        let selected = Services::parse(Some(" scout, subtitles ".into())).unwrap();
        assert!(selected.has(Service::Scout));
        assert!(selected.has(Service::Subs));
        assert!(!selected.has(Service::Reel));
        assert!(!selected.den());
    }

    #[test]
    fn typos_and_empty_lists_are_errors() {
        assert!(Services::parse(Some("scuot".into())).unwrap_err().contains("scuot"));
        assert!(Services::parse(Some(",,".into())).is_err());
    }
}
