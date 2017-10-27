use std::thread;
use std::sync::{Arc, Mutex};

use websocket::OwnedMessage;
use websocket::sync::Server;

use std::iter::Iterator;

use std::borrow::Borrow;

use language::*;

impl Relation {
    fn sorted(&self, ordering: &[usize]) -> Relation {
        let len = if self.columns.len() > 0 {
            self.columns[0].len()
        } else {
            0
        };
        let mut ixes = (0..len).collect::<Vec<_>>();
        for &c in ordering.iter().rev() {
            // stable sort
            ixes.sort_by(|&r1, &r2| self.columns[c][r1].cmp(&self.columns[c][r2]));
        }
        let sorted_columns = self.columns
            .iter()
            .map(|column| ixes.iter().map(|&ix| column[ix].clone()).collect())
            .collect();
        Relation { columns: sorted_columns }
    }
}

// f should be `|t| t < value` or `|t| t <= value`
fn gallop<'a, T, F: Fn(&T) -> bool>(slice: &'a [T], mut lo: usize, hi: usize, f: F) -> usize {
    if lo < hi && f(&slice[lo]) {
        let mut step = 1;
        while lo + step < hi && f(&slice[lo + step]) {
            lo = lo + step;
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if lo + step < hi && f(&slice[lo + step]) {
                lo = lo + step;
            }
            step = step >> 1;
        }

        lo += 1
    }
    lo
}

type LoHi = (usize, usize);

fn constrain<'a>(
    constraints: &[Constraint],
    indexes: &'a [Relation],
    ranges: &mut [LoHi],
    variables: &mut [Value<'a>],
    result_vars: &[(String, usize)],
    results: &mut Vec<Value>,
) -> Result<(), String> {
    if constraints.len() > 0 {
        match &constraints[0] {
            &Constraint::Join(var_ix, result_already_fixed, ref rowcols) => {
                let mut buffer = vec![(0, 0); rowcols.len()]; // TODO pre-allocate
                if result_already_fixed {
                    // loop over rowcols[0..]
                    let mut i = 0;
                    {
                        let value = variables[var_ix].borrow();
                        while i < rowcols.len() {
                            let (row_ix, col_ix) = rowcols[i];
                            let column = &indexes[row_ix].columns[col_ix];
                            let (old_lo, old_hi) = ranges[row_ix];
                            let lo = gallop(column, old_lo, old_hi, |v| v < value);
                            let hi = gallop(column, lo, old_hi, |v| v <= value);
                            if lo < hi {
                                ranges[row_ix] = (lo, hi);
                                buffer[i] = (old_lo, old_hi);
                                i += 1;
                            } else {
                                break;
                            }
                        }
                    }
                    // if all succeeded, continue with rest of constraints
                    if i == rowcols.len() {
                        constrain(
                            &constraints[1..],
                            indexes,
                            ranges,
                            variables,
                            result_vars,
                            results,
                        )?;
                    }
                    // restore state for rowcols[0..i]
                    while i > 0 {
                        i -= 1;
                        let (row_ix, _) = rowcols[i];
                        ranges[row_ix] = buffer[i];
                    }
                } else {
                    let (row_ix, col_ix) = rowcols[0]; // TODO pick smallest
                    let column = &indexes[row_ix].columns[col_ix];
                    let (old_lo, old_hi) = ranges[row_ix];
                    let mut lo = old_lo;
                    // loop over rowcols[0]
                    while lo < old_hi {
                        let value = &column[lo];
                        let hi = gallop(column, lo + 1, old_hi, |v| v <= value);
                        ranges[row_ix] = (lo, hi);
                        {
                            // loop over rowcols[1..]
                            let mut i = 1;
                            while i < rowcols.len() {
                                let (row_ix, col_ix) = rowcols[i];
                                let column = &indexes[row_ix].columns[col_ix];
                                let (old_lo, old_hi) = ranges[row_ix];
                                let lo = gallop(column, old_lo, old_hi, |v| v < value);
                                let hi = gallop(column, lo, old_hi, |v| v <= value);
                                if lo < hi {
                                    ranges[row_ix] = (lo, hi);
                                    buffer[i] = (old_lo, old_hi);
                                    i += 1;
                                } else {
                                    break;
                                }
                            }
                            // if all succeeded, continue with rest of constraints
                            if i == rowcols.len() {
                                variables[var_ix] = column[lo].really_borrow();
                                constrain(
                                    &constraints[1..],
                                    indexes,
                                    ranges,
                                    variables,
                                    result_vars,
                                    results,
                                )?;
                            }
                            // restore state for rowcols[1..i]
                            while i > 1 {
                                i -= 1;
                                let (row_ix, _) = rowcols[i];
                                ranges[row_ix] = buffer[i];
                            }
                        }
                        lo = hi;
                    }
                    // restore state for rowcols[0]
                    ranges[row_ix] = (old_lo, old_hi);
                }
            }
            &Constraint::Apply(result_ix, result_already_fixed, ref function) => {
                let result = function.apply(variables)?;
                if result_already_fixed {
                    if variables[result_ix] == result {
                        constrain(
                            &constraints[1..],
                            indexes,
                            ranges,
                            variables,
                            result_vars,
                            results,
                        )?;
                    } else {
                        // failed, backtrack
                    }
                } else {
                    variables[result_ix] = result;
                    constrain(
                        &constraints[1..],
                        indexes,
                        ranges,
                        variables,
                        result_vars,
                        results,
                    )?;
                }
            }
        }
    } else {
        for &(_, var_ix) in result_vars.iter() {
            results.push(variables[var_ix].really_to_owned());
        }
    }
    Ok(())
}

impl Block {
    fn run(&self, db: &DB) -> Result<Vec<Value<'static>>, String> {
        let mut indexes: Vec<Relation> = vec![];
        for (name, ordering) in self.row_names.iter().zip(self.row_orderings.iter()) {
            indexes.push(
                db.relations
                    .get(name)
                    .ok_or_else(|| format!("Couldn't find relation: {}", name))?
                    .sorted(ordering),
            )
        }
        let mut variables: Vec<Value> = self.variables.clone();
        let mut ranges: Vec<LoHi> = indexes
            .iter()
            .map(|index| (0, index.columns[0].len()))
            .collect();
        let mut results = vec![];
        constrain(
            &*self.constraints,
            &*indexes,
            &mut *ranges,
            &mut *variables,
            &*self.result_vars,
            &mut results,
        )?;
        Ok(results)
    }
}

pub fn run_code(db: &DB, code: &str, cursor: i64) {
    let code_ast = code_ast(code, cursor);

    let mut status: Vec<Result<(Block, Vec<Value>), String>> = vec![];
    for block in code_ast.blocks.iter() {
        match block {
            &Err(ref error) => status.push(Err(format!("Parse error: {}", error))),
            &Ok(ref block) => {
                print!("{:?}\n\n", block);
                match plan(block) {
                    Err(error) => status.push(Err(format!("Compile error: {}", error))),
                    Ok(block) => {
                        print!("{:?}\n\n", block);
                        match block.run(db) {
                            Err(error) => status.push(Err(format!("Run error: {}", error))),
                            Ok(results) => {
                                status.push(Ok((block, results)));
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(ix) = code_ast.focused {
        match &status[ix] {
            &Err(ref error) => print!("{}\n\n", error),
            &Ok((ref block, ref results)) => {
                let result_vars = &block.result_vars;

                print!(
                    "Ok: {} results\n\n",
                    if result_vars.len() > 0 {
                        results.len() / result_vars.len()
                    } else {
                        0
                    }
                );

                if result_vars.len() > 0 {
                    for (i, row) in results.chunks(result_vars.len()).take(10).enumerate() {
                        for (&(ref name, _), value) in result_vars.iter().zip(row.iter()) {
                            print!("{}={}\t", name, value);
                        }
                        if i == 9 {
                            print!("...\n");
                        } else {
                            print!("\n");
                        }
                    }
                    print!("\n");
                }

                print!("{:?}\n\n{:?}\n\n", code_ast.blocks[ix], block);
            }
        }

    } else {
        print!("Nothing focused\n\n");
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum EditorEvent {
    State(String, i64),
}

// #[derive(Debug, Serialize, Deserialize)]
// enum Command {
//     Render(String),
// }

// fn send_command(sender: &mut websocket::sender::Writer<std::net::TcpStream>, c: Command) {
//     sender
//         .send_message(&OwnedMessage::Text(json!(c).to_string()))
//         .unwrap()
// }

pub fn serve_editor() {
    let state = Arc::new(Mutex::new(("".to_owned(), 0)));

    let server = Server::bind("127.0.0.1:8081").unwrap();

    thread::spawn({
        let state = state.clone();
        move || {
            let db = load_chinook().unwrap();
            println!("DB is {:?}", db);
            let mut last_state = state.lock().unwrap().clone();
            loop {
                let state: (String, i64) = state.lock().unwrap().clone();
                if state != last_state {
                    print!("\x1b[2J\x1b[1;1H");
                    let (ref code, cursor) = state;
                    let start = ::std::time::Instant::now();
                    run_code(&mut db.clone(), &*code, cursor);
                    let elapsed = start.elapsed();
                    println!(
                        "In {} ms",
                        (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64
                    );
                    last_state = state.clone();
                }
            }
        }
    });

    for request in server.filter_map(Result::ok) {
        let state = state.clone();
        thread::spawn(move || {
            let client = request.accept().unwrap();
            let ip = client.peer_addr().unwrap();
            println!("Connection from {}", ip);

            let (mut receiver, mut sender) = client.split().unwrap();

            for message in receiver.incoming_messages() {
                let message = message.unwrap();

                match message {
                    OwnedMessage::Close(_) => {
                        let message = OwnedMessage::Close(None);
                        sender.send_message(&message).unwrap();
                        println!("Client {} disconnected", ip);
                        return;
                    }
                    OwnedMessage::Ping(ping) => {
                        let message = OwnedMessage::Pong(ping);
                        sender.send_message(&message).unwrap();
                    }
                    OwnedMessage::Text(ref text) => {
                        // println!("Received: {}", text);
                        let event: EditorEvent = ::serde_json::from_str(text).unwrap();
                        match event {
                            EditorEvent::State(code, cursor) => {
                                *state.lock().unwrap() = (code, cursor);
                            }
                        }
                    }
                    _ => {
                        panic!("A weird message! {:?}", message);
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let col = (0..1000).collect::<Vec<_>>();
        for i in 0..1000 {
            assert_eq!(col[gallop(&*col, 0, 1000, |&x| x < i)], i);
        }
        assert_eq!(gallop(&*col, 0, col.len(), |&x| x < -1), 0);
        assert_eq!(gallop(&*col, 0, col.len(), |&x| x < 1000), 1000);

        for i in 0..999 {
            assert_eq!(col[gallop(&*col, 0, col.len(), |&x| x <= i)], i + 1);
        }
        assert_eq!(gallop(&*col, 0, col.len(), |&x| x <= -1), 0);
        assert_eq!(gallop(&*col, 0, col.len(), |&x| x <= 999), 1000);
    }
}