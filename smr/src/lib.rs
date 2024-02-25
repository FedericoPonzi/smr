trait Command {}
trait Result {}
trait Applicator {}

trait StateMachineReplication<C: Command, R: Result, A: Applicator> {
    fn propose(command: C) -> R;
    fn sync();
    fn register_apply(applicator: A);
}
