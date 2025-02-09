use automerge::{InvalidChangeRequest, LocalChange, Path, Primitive, Value};
use automerge_persistent::PersistentBackend;
use criterion::{criterion_group, criterion_main, Criterion};

fn small_backend_apply_local_change(c: &mut Criterion) {
    c.bench_function("small backend apply local change", |b| {
        b.iter_batched(
            || {
                let db = sled::Config::new().temporary(true).open().unwrap();
                let sled = automerge_persistent_sled::SledPersister::new(
                    db.open_tree("changes").unwrap(),
                    db.open_tree("document").unwrap(),
                    db.open_tree("sync_states").unwrap(),
                    "".to_owned(),
                )
                .unwrap();
                let backend: PersistentBackend<
                    automerge_persistent_sled::SledPersister,
                    automerge::Backend,
                > = automerge_persistent::PersistentBackend::load(sled).unwrap();
                let mut frontend = automerge::Frontend::new();
                let ((), change) = frontend
                    .change::<_, _, InvalidChangeRequest>(None, |doc| {
                        doc.add_change(LocalChange::set(
                            Path::root().key("a"),
                            Value::Primitive(Primitive::Str("abcdef".into())),
                        ))
                        .unwrap();
                        Ok(())
                    })
                    .unwrap();

                (backend, change.unwrap())
            },
            |(mut persistent_doc, change)| persistent_doc.apply_local_change(change),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn small_backend_apply_local_change_flush(c: &mut Criterion) {
    c.bench_function("small backend apply local change flush", |b| {
        b.iter_batched(
            || {
                let db = sled::Config::new().temporary(true).open().unwrap();
                let sled = automerge_persistent_sled::SledPersister::new(
                    db.open_tree("changes").unwrap(),
                    db.open_tree("document").unwrap(),
                    db.open_tree("sync_states").unwrap(),
                    "".to_owned(),
                )
                .unwrap();
                let backend: PersistentBackend<
                    automerge_persistent_sled::SledPersister,
                    automerge::Backend,
                > = automerge_persistent::PersistentBackend::load(sled).unwrap();
                let mut frontend = automerge::Frontend::new();
                let ((), change) = frontend
                    .change::<_, _, InvalidChangeRequest>(None, |doc| {
                        doc.add_change(LocalChange::set(
                            Path::root().key("a"),
                            Value::Primitive(Primitive::Str("abcdef".into())),
                        ))
                        .unwrap();
                        Ok(())
                    })
                    .unwrap();

                (db, backend, change.unwrap())
            },
            |(db, mut persistent_doc, change)| {
                persistent_doc.apply_local_change(change).unwrap();
                db.flush().unwrap()
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn small_backend_apply_changes(c: &mut Criterion) {
    c.bench_function("small backend apply changes", |b| {
        b.iter_batched(
            || {
                let db = sled::Config::new().temporary(true).open().unwrap();
                let sled = automerge_persistent_sled::SledPersister::new(
                    db.open_tree("changes").unwrap(),
                    db.open_tree("document").unwrap(),
                    db.open_tree("sync_states").unwrap(),
                    "".to_owned(),
                )
                .unwrap();
                let mut other_backend = automerge::Backend::new();
                let backend: PersistentBackend<
                    automerge_persistent_sled::SledPersister,
                    automerge::Backend,
                > = automerge_persistent::PersistentBackend::load(sled).unwrap();
                let mut frontend = automerge::Frontend::new();
                let ((), change) = frontend
                    .change::<_, _, InvalidChangeRequest>(None, |doc| {
                        doc.add_change(LocalChange::set(
                            Path::root().key("a"),
                            Value::Primitive(Primitive::Str("abcdef".into())),
                        ))
                        .unwrap();
                        Ok(())
                    })
                    .unwrap();
                let (_patch, _change) = other_backend.apply_local_change(change.unwrap()).unwrap();
                let changes = other_backend
                    .get_changes(&[])
                    .into_iter()
                    .cloned()
                    .collect();
                (backend, changes)
            },
            |(mut persistent_doc, changes)| persistent_doc.apply_changes(changes),
            criterion::BatchSize::SmallInput,
        )
    });
}

fn small_backend_compact(c: &mut Criterion) {
    c.bench_function("small backend compact", |b| {
        b.iter_batched(
            || {
                let db = sled::Config::new().temporary(true).open().unwrap();
                let sled = automerge_persistent_sled::SledPersister::new(
                    db.open_tree("changes").unwrap(),
                    db.open_tree("document").unwrap(),
                    db.open_tree("sync_states").unwrap(),
                    "".to_owned(),
                )
                .unwrap();
                let mut backend: PersistentBackend<
                    automerge_persistent_sled::SledPersister,
                    automerge::Backend,
                > = automerge_persistent::PersistentBackend::load(sled).unwrap();
                let mut frontend = automerge::Frontend::new();
                let ((), change) = frontend
                    .change::<_, _, InvalidChangeRequest>(None, |doc| {
                        doc.add_change(LocalChange::set(
                            Path::root().key("a"),
                            Value::Primitive(Primitive::Str("abcdef".into())),
                        ))
                        .unwrap();
                        Ok(())
                    })
                    .unwrap();
                let _patch = backend.apply_local_change(change.unwrap()).unwrap();
                backend
            },
            |mut persistent_doc| persistent_doc.compact(&[]),
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(50);
    targets = small_backend_apply_local_change, small_backend_apply_local_change_flush, small_backend_apply_changes, small_backend_compact
}
criterion_main!(benches);
