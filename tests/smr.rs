struct MyDatabase {
    data: Vec<String>,
}

enum Commands {
    Insert(u32, String),
    Get(u32),
}

#[test]
fn test_smr() -> anyhow::Result<()> {
    let db = MyDatabase { data: vec![] };
    /*let cmd = Commands::Insert(123, "Hello".to_string());
    let smr = Multipaxos::new<Commands>();
    smr.register_apply(db);
    smr.propose(cmd)?;
    assert_eq!(db.get(123), "Hello".to_string());
    */
    Ok(())
}
