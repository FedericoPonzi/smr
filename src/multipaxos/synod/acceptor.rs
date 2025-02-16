/**
 ** * A1: an acceptor can only adopt strictly increasing ballot numbers;
 ** * A2: an acceptor α can only add 〈b, s, c〉 to α.accepted (i.p., accept 〈b, s, c〉) if b = α.ballot num;
 ** * A3: acceptor α cannot remove pvalues from α.accepted (we will modify this impractical restriction later);
 ** * A4: Suppose α and α′ are acceptors, with 〈b, s, c〉 ∈ α.accepted and 〈b, s, c′〉 ∈ α′.accepted. Then c = c′.
 **  Informally, given a particular ballot number and slot number, there can be at most one proposed command under consideration by the set of acceptors.
 ** * A5: Suppose that for each α among a majority of acceptors, 〈b, s, c〉 ∈ α.accepted. If b′ > b and 〈b′, s, c′〉 ∈ α′.accepted, then c = c′.
 **  We will consider this crucial invariant in more detail later.
 **/
use crate::multipaxos::{
    Accept, AckAccept, Ballot, MaxAcceptedProposal, Prepare, Promise, SenderId,
};
use crate::CommandTrait;

#[derive(Debug, Clone)]
pub struct Acceptor<C>
where
    C: CommandTrait,
{
    pub my_id: SenderId,
    pub(crate) max_ballot: Ballot,
    pub(crate) max_accepted: Option<MaxAcceptedProposal<C>>,
}

impl<C> Acceptor<C>
where
    C: CommandTrait,
{
    pub fn new(my_id: SenderId) -> Self {
        Acceptor {
            max_ballot: 0,
            max_accepted: None,
            my_id,
        }
    }

    pub fn handle_prepare(&mut self, p: Prepare) -> Option<Promise<C>> {
        let is_safe_to_join_ballot = self.max_ballot < p.ballot;
        if is_safe_to_join_ballot {
            self.max_ballot = p.ballot;
            Some(Promise {
                sender: self.my_id,
                max_accepted: self.max_accepted.clone(),
            })
        } else {
            None
        }
    }

    pub fn handle_accept(&mut self, a: Accept<C>) -> Option<AckAccept> {
        if a.ballot == self.max_ballot {
            // TODO: strict equality or >=?
            self.max_accepted = Some(MaxAcceptedProposal {
                ballot: a.ballot,
                command: a.command,
            });
            Some(AckAccept {
                sender: self.my_id,
                ballot: a.ballot,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::multipaxos::{Accept, Acceptor, Prepare};
    /*
               TODO:
        * A ballot with max_ballot + 1 should succeed.
        * A ballot equal to max_ballot should fail (in handle_prepare).
        * Ensure that max_accepted does not change if an accept is rejected.

             */
    #[test]
    fn test_acceptor() {
        let mut acceptor = Acceptor::new(0);
        let response = acceptor.handle_prepare(Prepare {
            sender: 1,
            ballot: 1,
        });
        assert!(response.is_some());
        let accepted = Accept {
            sender: 1,
            ballot: 1,
            command: 1,
        };
        let accepted_response = acceptor.handle_accept(accepted);
        assert!(accepted_response.is_some());
        let accepted_response = accepted_response.unwrap();
        assert_eq!(accepted_response.ballot, 1);

        assert!(acceptor
            .handle_prepare(Prepare {
                sender: 2,
                ballot: 2,
            })
            .is_some());
        assert_eq!(acceptor.max_ballot, 2);
        // Should not promise to vote in a ballot smaller than the max ballot.
        assert!(acceptor
            .handle_prepare(Prepare {
                sender: 1,
                ballot: 1,
            })
            .is_none());
    }
}
