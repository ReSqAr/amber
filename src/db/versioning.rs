use serde::de;
use serde::ser::SerializeTuple;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum V1<T> {
    V1(T),
}

impl<T> std::ops::Deref for V1<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        match &self {
            V1::V1(v1) => v1,
        }
    }
}

impl<T> From<T> for V1<T> {
    fn from(v: T) -> Self {
        V1::V1(v)
    }
}

impl<T> V1<T> {
    pub fn into_inner(self) -> T {
        match self {
            V1::V1(v) => v,
        }
    }
}

impl<T: serde::Serialize> serde::Serialize for V1<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            V1::V1(inner) => {
                let mut tup = serializer.serialize_tuple(2)?;
                tup.serialize_element(&1u8)?;
                tup.serialize_element(inner)?;
                tup.end()
            }
        }
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for V1<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (ver, val) = <(u8, T)>::deserialize(deserializer).map_err(de::Error::custom)?;
        match ver {
            1 => Ok(V1::V1(val)),
            other => Err(de::Error::custom(format!("unknown version tag {other}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::versioning::V1;
    use serde::de;
    use serde::ser::SerializeTuple;
    use std::fmt;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
    struct Dummy {
        count: u32,
        label: String,
    }
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum V2<T, U> {
        V1(T),
        V2(U),
    }

    impl<T: serde::Serialize, U: serde::Serialize> serde::Serialize for V2<T, U> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut tup = serializer.serialize_tuple(2)?;
            match self {
                V2::V1(t) => {
                    tup.serialize_element(&1u8)?;
                    tup.serialize_element(t)?;
                }
                V2::V2(u) => {
                    tup.serialize_element(&2u8)?;
                    tup.serialize_element(u)?;
                }
            }
            tup.end()
        }
    }

    impl<'de, T: serde::Deserialize<'de>, U: serde::Deserialize<'de>> serde::Deserialize<'de>
        for V2<T, U>
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct V2Visitor<T, U>(std::marker::PhantomData<(T, U)>);

            impl<'de, T: serde::Deserialize<'de>, U: serde::Deserialize<'de>> de::Visitor<'de>
                for V2Visitor<T, U>
            {
                type Value = V2<T, U>;

                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    write!(f, "a tuple (version: u8, value)")
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: de::SeqAccess<'de>,
                {
                    let ver: u8 = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    match ver {
                        1 => {
                            let t: T = seq
                                .next_element()?
                                .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                            Ok(V2::V1(t))
                        }
                        2 => {
                            let u: U = seq
                                .next_element()?
                                .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                            Ok(V2::V2(u))
                        }
                        other => Err(de::Error::custom(format!("unknown version tag {other}"))),
                    }
                }
            }

            deserializer.deserialize_tuple(2, V2Visitor::<T, U>(std::marker::PhantomData))
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
    struct DummyV2 {
        count: u32,
        label: String,
        extra: bool,
    }

    #[test]
    fn postcard_bytes_from_one_version_can_be_read_by_two_versions() {
        let original = V1::V1(Dummy {
            count: 2127433642,
            label: "hello".to_string(),
        });

        let bytes = postcard::to_stdvec(&original).expect("serialize V<T>");

        let decoded: V2<Dummy, DummyV2> =
            postcard::from_bytes(&bytes).expect("deserialize into TwoVersions<T, U>");

        assert_eq!(
            decoded,
            V2::V1(Dummy {
                count: 2127433642,
                label: "hello".to_string(),
            })
        );
    }
}
