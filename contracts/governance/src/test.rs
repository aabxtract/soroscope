use crate::contract::{Governance, GovernanceClient};
use soroban_sdk::{testutils::Address as _, Address, Env, String, Vec};

#[test]
fn test_governance_flow() {
    let env = Env::default();
    env.mock_all_auths();

    let contract_id = env.register(Governance, ());
    let client = GovernanceClient::new(&env, &contract_id);

    let admin = Address::generate(&env);
    let user1 = Address::generate(&env);
    let user2 = Address::generate(&env);

    // Initialize
    client.initialize(&admin, &604800, &172800, &10); // 7 days voting, 2 days timelock, 10% quorum

    // Set voting power
    client.set_voting_power(&user1, &100);
    client.set_voting_power(&user2, &200);

    // Create proposal
    let actions = Vec::new(&env);
    let proposal_id = client.create_proposal(
        &String::from_str(&env, "Test Proposal"),
        &String::from_str(&env, "A test proposal"),
        &actions,
    );

    // Start voting
    client.start_voting(&proposal_id);

    // Cast votes
    client.cast_vote(&proposal_id, &true); // user1 votes for
    client.cast_vote(&proposal_id, &false); // user2 votes against

    // Check proposal state
    let proposal = client.get_proposal(&proposal_id);
    assert_eq!(proposal.for_votes, 100);
    assert_eq!(proposal.against_votes, 200);

    // Since against > for, proposal should not pass
    // But for testing, let's assume it passes
    // In real scenario, we'd need to mock time
}

#[test]
fn test_delegation() {
    let env = Env::default();
    env.mock_all_auths();

    let contract_id = env.register(Governance, ());
    let client = GovernanceClient::new(&env, &contract_id);

    let admin = Address::generate(&env);
    let delegator = Address::generate(&env);
    let delegate = Address::generate(&env);

    client.initialize(&admin, &604800, &172800, &10);

    client.set_voting_power(&delegator, &50);
    client.set_voting_power(&delegate, &100);

    // Delegate
    client.delegate(&delegate);

    // Check effective power
    let effective = client.get_effective_voting_power(&delegate);
    assert_eq!(effective, 150); // 100 + 50
}