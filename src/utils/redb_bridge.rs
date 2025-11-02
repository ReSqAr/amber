macro_rules! impl_redb_bincode_value {
    ($t:ty) => {
        impl redb::Value for $t
        where
            $t: serde::Serialize + for<'de> serde::Deserialize<'de>,
        {
            type SelfType<'a> = $t;
            type AsBytes<'a> = std::borrow::Cow<'a, [u8]>;

            fn fixed_width() -> Option<usize> {
                None
            }

            fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
            where
                Self: 'a,
            {
                bincode::Options::deserialize::<'a, Self::SelfType<'a>>(bincode::options(), data)
                    .expect("bincode decode failed for redb::Value")
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'a,
            {
                std::borrow::Cow::Owned(
                    bincode::Options::serialize(bincode::options(), value)
                        .expect("bincode encode failed for redb::Value"),
                )
            }

            fn type_name() -> redb::TypeName {
                redb::TypeName::new(std::any::type_name::<$t>())
            }
        }
    };
}

macro_rules! impl_redb_bincode_key {
    ($t:ty) => {
        impl_redb_bincode_value!($t);

        impl redb::Key for $t
        where
            $t: serde::Serialize + for<'de> serde::Deserialize<'de>,
        {
            fn compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
                a.cmp(b)
            }
        }
    };
}

pub(crate) use impl_redb_bincode_key;
pub(crate) use impl_redb_bincode_value;
