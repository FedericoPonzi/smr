use crate::channel::Channel;
use crate::synod::MaxAcceptedProposal;
/**
 ** * A1: an acceptor can only adopt strictly increasing ballot numbers;
 ** * A2: an acceptor α can only add 〈b, s, c〉 to α.accepted (i.p., accept 〈b, s, c〉) if b = α.ballot num;
 ** * A3: acceptor α cannot remove pvalues from α.accepted (we will modify this impractical restriction later);
 ** * A4: Suppose α and α′ are acceptors, with 〈b, s, c〉 ∈ α.accepted and 〈b, s, c′〉 ∈ α′.accepted. Then c = c′.
 **     Informally, given a particular ballot num- ber and slot number, there can be at most one proposed command under consideration by the set of acceptors.
 ** * A5: Suppose that for each α among a majority of acceptors, 〈b, s, c〉 ∈ α.accepted. If b′ > b and 〈b′, s, c′〉 ∈ α′.accepted, then c = c′.
 **     We will consider this crucial invariant in more detail later.
 **/
use crate::{Accept, Accepted, Ballot, Promise, Proposal};

pub struct Acceptor {
    pub(crate) max_ballot: Ballot,
    max_accepted: Option<MaxAcceptedProposal>,
}
impl Acceptor {
    fn is_safe_to_join_ballot(&self, ballot: Ballot) -> bool {
        self.max_ballot < ballot
    }

    pub fn new() -> Self {
        Acceptor {
            max_ballot: 0,
            max_accepted: None,
        }
    }

    pub fn handle_proposal(&self, p: Proposal) -> Option<Promise> {
        self.is_safe_to_join_ballot(p.ballot).then(|| Promise {
            max_accepted: self.max_accepted.clone(),
        })
    }

    pub fn handle_accept(&mut self, a: Accept) -> Option<Accepted> {
        if a.ballot >= self.max_ballot {
            self.max_ballot = a.ballot;
            self.max_accepted = Some(MaxAcceptedProposal(a.ballot, a.value));
            Some(Accepted {
                ballot: a.ballot,
                value: a.value,
            })
        } else {
            None
        }
    }
    pub fn run<C: Channel>(channel: C) -> anyhow::Result<()> {
        todo!();
    }
}
impl Default for Acceptor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use crate::{Accept, Proposal};

    #[test]
    fn test_acceptor() {
        let mut acceptor = crate::Acceptor::new();
        let response = acceptor.handle_proposal(Proposal { ballot: 1 });
        assert!(response.is_some());
        let accepted = Accept {
            ballot: 1,
            value: 1,
        };
        let accepted_response = acceptor.handle_accept(accepted);
        assert!(accepted_response.is_some());
        let accepted_response = accepted_response.unwrap();
        assert_eq!(accepted_response.ballot, 1);
        assert_eq!(accepted_response.value, 1);

        assert!(acceptor.handle_proposal(Proposal { ballot: 2 }).is_some());
        assert_eq!(acceptor.max_ballot, 1);
        // Should not promise to vote in a ballot smaller than the max ballot.
        assert!(acceptor.handle_proposal(Proposal { ballot: 1 }).is_none());
    }
}
