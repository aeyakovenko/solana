/// Tic-Tac-Toe smart contract
/// Players can create a game, play it, contract can maintaing their ranking
use bincode::{deserialize, serialize_into};
use signature::PublicKey;
use std::io::Cursor;

/// smart contract interface
/// * `instance` - read only module instance data, currently empty, but shoudl
///                be filled with data supplied by the contract instance
///                creator
/// * `method` - just like a system call, this is the index of the method
///              that is called in this contract
/// * `mems` - buffers passed in to be modified by the contract.  The following
///            indexies indicate different kinds of buffers:
///     `CALLER` - context for the creator of the transaction that made the `call`
///     `CALL_DATA` - copy of userdata passed into the transaction
///     `MEM_START` - buffers that are paged in for this call
/// TBD: We can pass a Vec<Call> to the driver as well, which the driver can
/// fill out as asynchronous calls to be scheduled after this call.  Ideally
/// this should be something coalescable.
pub fn call(
    // module instance data, currently empty, but should be filled with
    // data supplied by the contract instance creator
    instance: &Instance,
    // method
    method: i64,
    // memory for the call
    mems: &mut Vec<CallData>,
) {
    match method {
        0 => new_player(instance, mems),
        1 => new_game(instance, mems),
        2 => take_turn(instance, mems),
        _ => (),
    }
}

/// raw memory
pub type Memory = Vec<u8>;

/// caller is always the first mem index
const CALLER: usize = 0;
/// call_data is transaction data, public_key == [0..], bit flags are ignored
const CALL_DATA: usize = 1;
/// mem_start is the start of the memory buffers paged into this call
const MEM_START: usize = 2;

pub struct CallData {
    /// public key that is the address of the memory
    public_key: PublicKey,
    /// memory that is identified by the public key
    mem: Memory,
    /// `true` if the incomming transaction prooved ownershp of public_key
    proof_of_ownership: bool,
    /// `true` if the memory was just created
    just_allocated: bool,
}

pub struct Instance {
    //read only config data
}

/// game specific code
#[derive(Serialize, Deserialize, PartialEq, Clone)]
enum Kind {
    Uninitialized,
    Player,
    Game,
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
struct Player {
    kind: Kind,
    rank: u64,
    public_key: PublicKey,
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
struct Game {
    kind: Kind,
    ///copy of Player at the start of the game
    ///this allows us to track the initial ranks of the players at the start of the game
    player_one: Player,
    player_two: Player,
    player_one_turn: bool,
    board: [u8; 9],
    over: bool,
}

fn new_player(_instance: &Instance, mems: &mut Vec<CallData>) {
    let caller_data = &mut mems[CALLER];
    assert!(caller_data.proof_of_ownership);
    assert!(caller_data.just_allocated);
    let p = Player {
        kind: Kind::Player,
        rank: 10000000,
        public_key: caller_data.public_key,
    };
    //should be just a cast in C
    let mut out = Cursor::new(&mut caller_data.mem);
    serialize_into(&mut out, &p).expect("failed to serialize output");
}

fn new_game(_instance: &Instance, mems: &mut Vec<CallData>) {
    let (player_one_data, player_two_data, game_data) = unsafe {
        //borrow checker can't handle two refs to indexies in the same function
        let a = &mut *(mems.get_unchecked_mut(CALLER) as *mut CallData);
        let b = &mut *(mems.get_unchecked_mut(MEM_START) as *mut CallData);
        let c = &mut *(mems.get_unchecked_mut(MEM_START + 1) as *mut CallData);
        (a, b, c)
    };
    assert!(player_one_data.proof_of_ownership);
    assert!(false == player_one_data.just_allocated);

    let player_one: Player = deserialize(&player_one_data.mem).unwrap();
    assert!(player_one.kind == Kind::Player);

    assert!(player_two_data.proof_of_ownership);
    assert!(false == player_two_data.just_allocated);

    let player_two: Player = deserialize(&player_two_data.mem).unwrap();
    assert!(player_two.kind == Kind::Player);

    //it doesn't matter who prooves the ownership of the key
    assert!(game_data.proof_of_ownership);
    //new game must be allocated for this call
    assert!(game_data.just_allocated);

    let g = Game {
        kind: Kind::Game,
        over: false,
        player_one: player_one.clone(),
        player_two: player_two.clone(),
        player_one_turn: true,
        board: [0u8; 9],
    };
    //should be just a cast in C
    let mut out = Cursor::new(&mut game_data.mem);
    serialize_into(&mut out, &g).expect("failed to serialize output");
}

fn take_turn(_instance: &Instance, mems: &mut Vec<CallData>) {
    let (player_data, game_data) = unsafe {
        //borrow checker can't handle two refs to indexies in the same function
        let a = &mut *(mems.get_unchecked_mut(CALLER) as *mut CallData);
        let b = &mut *(mems.get_unchecked_mut(MEM_START) as *mut CallData);
        (a, b)
    };
    assert!(player_data.proof_of_ownership);
    assert!(false == player_data.just_allocated);

    let player: Player = deserialize(&player_data.mem).unwrap();
    assert!(player.kind == Kind::Player);

    assert!(false == game_data.just_allocated);
    let mut game: Game = deserialize(&game_data.mem).unwrap();
    assert!(game.kind == Kind::Game);
    assert!(game.over == false);

    assert!(
        player_data.public_key == game.player_one.public_key
            || player_data.public_key == game.player_two.public_key
    );

    let call_data = &mems[CALL_DATA];
    let action: usize = deserialize(&call_data.mem).unwrap();
    assert!(action < game.board.len());
    assert!(game.board[action] == 0);

    if game.player_one_turn && player_data.public_key == game.player_one.public_key {
        game.board[action] = 1 as u8;
    } else if !game.player_one_turn && player_data.public_key == game.player_two.public_key {
        game.board[action] = 2 as u8;
    }

    let mut win = false;
    let mut tie = true;
    let diags = [
        [0, 1, 2],
        [3, 4, 5],
        [6, 7, 8],
        [0, 3, 6],
        [1, 4, 7],
        [2, 5, 8],
        [0, 4, 8],
        [2, 4, 6],
    ];
    //check for winner
    for diag in &diags {
        for d in diag {
            if game.board[action] == game.board[*d] {
                win = true;
            }
        }
    }
    //check for tie
    for d in &game.board {
        if *d == 0 {
            tie = false;
        }
    }
    if win || tie {
        game.over = true;
    }
    let mut out = Cursor::new(&mut game_data.mem);
    serialize_into(&mut out, &game).expect("failed to serialize output");
}
