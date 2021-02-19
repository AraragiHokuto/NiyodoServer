#![feature(proc_macro_hygiene, decl_macro)]

use chrono::NaiveDateTime;
use rocket::{get, routes};
use rocket_contrib::database;
use rocket_contrib::databases::postgres;
use rocket_contrib::json::Json;
use serde::Serialize;
use std::cmp::min;
use std::convert::TryInto;
use std::error::Error;

#[derive(Serialize)]
struct MsgEntry {
	id: i32,
	msg_type: String,
	sender: String,
	content: String,
	time: i64,
}

#[derive(Serialize)]
struct ListResponse {
	result: Vec<MsgEntry>,
}

#[database("niyodo")]
struct NiyodoDbConn(postgres::Connection);

const UPPER_LIMIT :i64 = 500;

#[get("/list?<time>&<limit>")]
fn list(
	conn: NiyodoDbConn,
	time: i64,
	limit: i64,
) -> Result<Json<ListResponse>, Box<dyn Error>> {
	let mut ret = Vec::<MsgEntry>::with_capacity(limit.try_into()?);

	for row in &conn.query(
		"SELECT id, type, sender, content, datetime \
                         FROM message \
                         WHERE datetime >= $1 \
                         ORDER BY datetime, id \
                         LIMIT $2",
		&[
			&NaiveDateTime::from_timestamp(time, 0),
			&min(limit, UPPER_LIMIT),
		],
	)? {
		let datetime = row.get::<&str, NaiveDateTime>("datetime");

		ret.push(MsgEntry {
			id: row.get("id"),
			msg_type: row.get("type"),
			sender: row.get("sender"),
			content: row.get("content"),
			time: datetime.timestamp(),
		});
	}
	Ok(Json(ListResponse { result: ret }))
}

#[get("/list_backwards?<time>&<limit>")]
fn list_backwards(
	conn: NiyodoDbConn,
	time: i64,
	limit: i64,
) -> Result<Json<ListResponse>, Box<dyn Error>> {
	let mut ret = Vec::<MsgEntry>::with_capacity(limit.try_into()?);

	for row in &conn.query(
		"SELECT id, type, sender, content, datetime \
                         FROM message \
                         WHERE datetime <= $1 \
                         ORDER BY datetime DESC, id DESC \
                         LIMIT $2",
		&[
			&NaiveDateTime::from_timestamp(time, 0),
			&min(limit, UPPER_LIMIT),
		],
	)? {
		let datetime = row.get::<&str, NaiveDateTime>("datetime");

		ret.push(MsgEntry {
			id: row.get("id"),
			msg_type: row.get("type"),
			sender: row.get("sender"),
			content: row.get("content"),
			time: datetime.timestamp(),
		});
	}
    ret.reverse();
	Ok(Json(ListResponse { result: ret }))
}

fn main() {
	rocket::ignite()
		.attach(NiyodoDbConn::fairing())
		.mount("/", routes![list, list_backwards])
		.launch();
}
