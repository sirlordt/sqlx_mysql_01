use sqlx::pool::PoolConnection;
use sqlx::{Acquire, Error, MySql, Pool, Transaction}; //, MySqlPool, MySqlConnection

use sqlx::mysql::MySqlPoolOptions;

use async_std::task;

use futures::{Stream, TryStreamExt};

//use sqlx::Row;

use sqlx::{Column, Row, TypeInfo};

//use std::borrow::{Borrow, BorrowMut};
//use std::cell::RefCell;
use std::process;
//use std::rc::Rc;

use serde_json::json;

use std::collections::HashMap;

async fn connect() -> Result<Pool<MySql>, Error> {
    //return MySqlPool::connect("mysql://root:dsistemas@localhost:3306/Test01DB").await;
    return MySqlPoolOptions::new()
        .max_connections(5)
        .connect("mysql://root:dummypass@localhost:3306/Test01DB")
        .await;
}

async fn do_test_connection() -> Option<Pool<MySql>> {
    let result = task::block_on(connect());

    match result {
        Err(err) => {
            println!("Cannot connect to database [{}]", err.to_string());
            //std::process::exit(1);
            None
        }

        Ok(pool) => {
            println!("Connected to database successfully.");

            /*
            let name = "test01";

            let mut rows = sqlx::query("SELECT * FROM users WHERE email = ?")
                .bind(name)
                .fetch(&pool);

            while let Ok(row) = rows.try_next().await {
                // map the row into a user-defined domain type
                let ext = row.unwrap().try_get("email");
                let email: &str = ext.unwrap();

                println!("{}", email);
            }
            */

            // let query_result = sqlx::query("Select * From sysUserGroup")
            //     .fetch_all(&pool)
            //     .await
            //     .unwrap();

            // println!("Number of Employees selected: {}", query_result.len());

            // //let format = format_description!("[day]/[month]/[year]");

            // for (rindex, row) in query_result.iter().enumerate() {
            //     let x: String = row.get(0);

            //     println!("Name => {}", x);
            //     /*
            //     println!("{}. No.: {}, Birth Date: {}, First Name: {}, Last Name: {}, Gender: {}, Hire Date: {}",
            //         rindex+1,
            //         &employee.emp_no,
            //         &employee.birth_date.format(&format).unwrap(),
            //         &employee.first_name,
            //         &employee.last_name,
            //         &employee.gender,
            //         &employee.hire_date.format(&format).unwrap());
            //         */
            // }

            Some(pool)
        }
    }
}

async fn do_query_01(connection_pool: &Option<Pool<MySql>>, query: &str) -> Result<(), Error> {
    match connection_pool {
        Some(connection_pool) => {
            let query_result = sqlx::query(query).fetch_all(connection_pool).await?;

            // let query_result = sqlx::query("Select * From sysUserGroup")
            //     .fetch_all(&connection_pool)
            //     .await?;

            println!("Number Of User Group Selected: {}", query_result.len());

            for (rindex, row) in query_result.iter().enumerate() {
                for (_cindex, column) in row.columns().iter().enumerate() {
                    println!("Col Name => {}", column.name());
                    println!("Col Index => {}", column.ordinal());
                }

                let x: String = row.get(0);

                println!("Row Index => {}", rindex);
                println!("Name => {}", x);
            }

            Ok(())
        }
        None => {
            //println!( "No connection pool provided" );
            //Err( //std::io::Error::new( std::io::ErrorKind::NotConnected, "No connection pool provided" ) )
            Err(Error::PoolClosed)
        }
    }
}

async fn do_query_02(connection_pool: &Option<Pool<MySql>>, query: &str) -> Result<(), Error> {
    match connection_pool {
        Some(connection_pool) => {
            //let name = "sirlordt";

            //let mut rows = sqlx::query(query).bind(name).fetch(connection_pool);
            let mut rows = sqlx::query(query).fetch(connection_pool);

            while let Some(row) = rows.try_next().await? {
                // map the row into a user-defined domain type
                let id: &str = row.try_get("Id")?;
                let name: &str = row.try_get("Name")?;
                println!("id: {}, name: {}", id, name);
            }

            Ok(())
        }
        None => {
            //println!( "No connection pool provided" );
            //Err( //std::io::Error::new( std::io::ErrorKind::NotConnected, "No connection pool provided" ) )
            Err(Error::PoolClosed)
        }
    }
}

async fn do_query_03(connection_pool: &Option<Pool<MySql>>, query: &str) -> Result<(), Error> {
    match connection_pool {
        Some(connection_pool) => {
            //let name = "sirlordt";

            //let mut db_connection = &mut connection_pool.acquire().await?;

            //let mut rows = sqlx::query(query).bind(name).fetch(connection_pool);
            let mut conn = connection_pool.acquire().await?;

            let mut rows = sqlx::query(query).fetch(&mut *conn);

            while let Some(row) = rows.try_next().await? {
                // map the row into a user-defined domain type
                let id: &str = row.try_get("Id")?;
                let name: &str = row.try_get("Name")?;
                println!("id: {}, name: {}", id, name);
            }

            Ok(())
        }
        None => {
            //println!( "No connection pool provided" );
            //Err( //std::io::Error::new( std::io::ErrorKind::NotConnected, "No connection pool provided" ) )
            Err(Error::PoolClosed)
        }
    }
}

async fn do_query_04(connection_pool: &Option<Pool<MySql>>, query: &str) -> Result<(), Error> {
    match connection_pool {
        Some(connection_pool) => {
            let mut conn = match connection_pool.acquire().await {
                Ok(conn) => conn,
                Err(error) => return Err(error),
            };

            let x = &mut *conn;
            let mut rows = sqlx::query(query).fetch(x);

            loop {
                let row = match rows.try_next().await {
                    Ok(optional) => match optional {
                        Some(row) => row,
                        None => {
                            break; //No more records.
                        }
                    },
                    Err(error) => {
                        eprintln!("Error => {:?}", error);
                        return Err(error);
                    }
                };

                let id: &str = row.try_get("Id")?;
                let name: &str = row.try_get("Name")?;
                println!("id: {}, name: {}", id, name);
            }

            // while let Some(row) = rows.try_next().await? {
            //     // map the row into a user-defined domain type
            //     let id: &str = row.try_get("Id")?;
            //     let name: &str = row.try_get("Name")?;
            //     println!("id: {}, name: {}", id, name);
            // }

            Ok(())
        }
        None => {
            //println!( "No connection pool provided" );
            //Err( //std::io::Error::new( std::io::ErrorKind::NotConnected, "No connection pool provided" ) )
            Err(Error::PoolClosed)
        }
    }
}

async fn do_query_05(connection_pool: &Option<Pool<MySql>>, query: &str) -> Result<(), Error> {
    match connection_pool {
        Some(connection_pool) => {
            let result: Vec<_> = sqlx::query(query)
                .fetch_all(connection_pool)
                .await
                .unwrap()
                .into_iter()
                .map(|row| {
                    json!(row
                        .columns()
                        .into_iter()
                        .map(|column| {
                            //row.get
                            let ordinal = column.ordinal();
                            let type_name = column.type_info().name();
                            (
                                column.name(),
                                match type_name {
                                    "TEXT" | "VARCHAR" | "JSON" => {
                                        json!(row.get::<Option<String>, _>(ordinal))
                                    }
                                    "INTEGER" => json!(row.get::<Option<i64>, _>(ordinal)),
                                    "BOOLEAN" => json!(row.get::<Option<bool>, _>(ordinal)),
                                    "REAL" => json!(row.get::<Option<f64>, _>(ordinal)),
                                    // probably missed a few other types?
                                    _ => {
                                        json!(format!("UNPROCESSED TYPE '{}'", type_name))
                                    }
                                },
                            )
                        })
                        .collect::<HashMap<_, _>>())
                })
                .collect();

            println!("{}", serde_json::to_string_pretty(&result).unwrap());

            // let query_result = sqlx::query("Select * From sysUserGroup")
            //     .fetch_all(&connection_pool)
            //     .await?;

            // println!("Number Of User Group Selected: {}", query_result.len());

            // for (rindex, row) in query_result.iter().enumerate() {

            //     for ( cindex,column) in row.columns().iter().enumerate() {

            //         println!("Col Name => {}", column.name() );
            //         println!("Col Index => {}", column.ordinal() );

            //     }

            //     let x: String = row.get(0);

            //     println!("Row Index => {}", rindex);
            //     println!("Name => {}", x);
            // }

            Ok(())
        }
        None => {
            //println!( "No connection pool provided" );
            //Err( //std::io::Error::new( std::io::ErrorKind::NotConnected, "No connection pool provided" ) )
            Err(Error::PoolClosed)
        }
    }
}

/*
async fn takes_txn(tx: &mut Transaction<'_, MySql>) -> anyhow::Result<()> {
    let row = sqlx::query("...").fetch_one(&mut **tx).await?;

    sqlx::query("...").execute(&mut **tx).await?;
}
*/

async fn do_query_06(connection_pool: &Option<Pool<MySql>>, query: &str) -> Result<(), Error> {
    match connection_pool {
        Some(connection_pool) => {
            let mut conn = match connection_pool.acquire().await {
                Ok(conn) => conn,
                Err(error) => return Err(error),
            };

            let mut trans = conn.begin().await?;

            {
                //let mut rows = sqlx::query(query).fetch(&mut *conn);

                let mut rows = sqlx::query(query).fetch(&mut *trans);

                loop {
                    let row = match rows.try_next().await {
                        Ok(optional) => match optional {
                            Some(row) => row,
                            None => {
                                break; //No more records.
                            }
                        },
                        Err(error) => {
                            eprintln!("Error => {:?}", error);
                            return Err(error);
                        }
                    };

                    let data = row
                        .columns()
                        .into_iter()
                        .map(|column| {
                            //row.get
                            let ordinal = column.ordinal();
                            let type_name = column.type_info().name();
                            (
                                column.name(),
                                match type_name {
                                    "TEXT" | "VARCHAR" | "JSON" | "CHAR" => {
                                        json!(row.get::<Option<String>, _>(ordinal))
                                    }
                                    "INTEGER" => json!(row.get::<Option<i64>, _>(ordinal)),
                                    "BOOLEAN" => json!(row.get::<Option<bool>, _>(ordinal)),
                                    "REAL" => json!(row.get::<Option<f64>, _>(ordinal)),
                                    // probably missed a few other types?
                                    _ => {
                                        json!(format!("UNPROCESSED TYPE '{}'", type_name))
                                    }
                                },
                            )
                        })
                        .collect::<HashMap<_, _>>();

                    println!("{}", serde_json::to_string_pretty(&data).unwrap());
                }
            }

            trans.commit().await?;

            // while let Some(row) = rows.try_next().await? {
            //     // map the row into a user-defined domain type
            //     let id: &str = row.try_get("Id")?;
            //     let name: &str = row.try_get("Name")?;
            //     println!("id: {}, name: {}", id, name);
            // }

            Ok(())
        }
        None => {
            //println!( "No connection pool provided" );
            //Err( //std::io::Error::new( std::io::ErrorKind::NotConnected, "No connection pool provided" ) )
            Err(Error::PoolClosed)
        }
    }
}

async fn do_query_07(
    connection: Option<&mut PoolConnection<MySql>>,
    transaction: Option<&mut Transaction<'_, MySql>>,
    query: &str,
) -> Result<Vec<HashMap<String, serde_json::Value>>, Error> {
    let current_connection = match connection {
        Some(connection) => Some(connection),
        None if transaction.is_none() => return Err(Error::PoolClosed),
        None => None,
    };

    let mut local_error: Option<_> = None;

    let mut result_data: Vec<HashMap<String, serde_json::Value>> = vec![];

    let mut process_data = |rows: &mut std::pin::Pin<
        Box<dyn Stream<Item = Result<sqlx::mysql::MySqlRow, Error>> + Send>,
    >|
     -> bool {
        let mut result = true;

        loop {
            let row = match task::block_on(rows.try_next()) {
                Ok(optional) => match optional {
                    Some(row) => row,
                    None => {
                        break; //No more records.
                    }
                },
                Err(error) => {
                    //println!("Error => {:?}", error);
                    //apply_transaction = false;
                    local_error = Some(error);
                    result = false;
                    //return Err(error);
                    break;
                }
            };

            ////let mut data: HashMap<String, serde_json::Value> = HashMap::new();

            // for column in row.columns().iter() {
            //     let ordinal = column.ordinal();
            //     let type_name = column.type_info().name();

            //     data.insert(
            //         column.name().to_string(),
            //         match type_name {
            //             "TEXT" | "VARCHAR" | "JSON" | "CHAR" => {
            //                 json!(row.get::<Option<String>, _>(ordinal))
            //             }
            //             "INTEGER" => json!(row.get::<Option<i64>, _>(ordinal)),
            //             "BOOLEAN" => json!(row.get::<Option<bool>, _>(ordinal)),
            //             "REAL" => json!(row.get::<Option<f64>, _>(ordinal)),
            //             // probably missed a few other types?
            //             _ => {
            //                 json!(format!("UNPROCESSED TYPE '{}'", type_name))
            //             }
            //         },
            //     );
            // }

            let data = row
                .columns()
                .into_iter()
                .map(|column| {
                    //row.get
                    let ordinal = column.ordinal();
                    let type_name = column.type_info().name();
                    (
                        column.name().to_string(),
                        match type_name {
                            "TEXT" | "VARCHAR" | "JSON" | "CHAR" => {
                                json!(row.get::<Option<String>, _>(ordinal))
                            }
                            "INTEGER" => json!(row.get::<Option<i64>, _>(ordinal)),
                            "BOOLEAN" => json!(row.get::<Option<bool>, _>(ordinal)),
                            "REAL" => json!(row.get::<Option<f64>, _>(ordinal)),
                            // probably missed a few other types?
                            _ => {
                                json!(format!("UNPROCESSED TYPE '{}'", type_name))
                            }
                        },
                    )
                        .to_owned()
                })
                .collect::<HashMap<_, _>>();

            result_data.push(data);
        }

        result
    };

    if transaction.is_none() {
        let apply_transaction;

        let mut local_transaction = current_connection.unwrap().begin().await?;

        {
            let real_conn = &mut *local_transaction;
            let mut rows = sqlx::query(query).fetch(real_conn);

            apply_transaction = process_data(&mut rows);

            /*
            loop {
                let row = match rows.try_next().await {
                    Ok(optional) => match optional {
                        Some(row) => row,
                        None => {
                            break; //No more records.
                        }
                    },
                    Err(error) => {
                        //println!("Error => {:?}", error);
                        apply_transaction = false;
                        local_error = Some(error);
                        //return Err(error);
                        break;
                    }
                };

                ////let mut data: HashMap<String, serde_json::Value> = HashMap::new();

                // for column in row.columns().iter() {
                //     let ordinal = column.ordinal();
                //     let type_name = column.type_info().name();

                //     data.insert(
                //         column.name().to_string(),
                //         match type_name {
                //             "TEXT" | "VARCHAR" | "JSON" | "CHAR" => {
                //                 json!(row.get::<Option<String>, _>(ordinal))
                //             }
                //             "INTEGER" => json!(row.get::<Option<i64>, _>(ordinal)),
                //             "BOOLEAN" => json!(row.get::<Option<bool>, _>(ordinal)),
                //             "REAL" => json!(row.get::<Option<f64>, _>(ordinal)),
                //             // probably missed a few other types?
                //             _ => {
                //                 json!(format!("UNPROCESSED TYPE '{}'", type_name))
                //             }
                //         },
                //     );
                // }

                let data = row
                    .columns()
                    .into_iter()
                    .map(|column| {
                        //row.get
                        let ordinal = column.ordinal();
                        let type_name = column.type_info().name();
                        (
                            column.name().to_string(),
                            match type_name {
                                "TEXT" | "VARCHAR" | "JSON" | "CHAR" => {
                                    json!(row.get::<Option<String>, _>(ordinal))
                                }
                                "INTEGER" => json!(row.get::<Option<i64>, _>(ordinal)),
                                "BOOLEAN" => json!(row.get::<Option<bool>, _>(ordinal)),
                                "REAL" => json!(row.get::<Option<f64>, _>(ordinal)),
                                // probably missed a few other types?
                                _ => {
                                    json!(format!("UNPROCESSED TYPE '{}'", type_name))
                                }
                            },
                        )
                            .to_owned()
                    })
                    .collect::<HashMap<_, _>>();

                result_data.push(data);
                //println!("{}", serde_json::to_string_pretty(&data).unwrap());
            }
            */
        }

        if apply_transaction {
            //my_other.as_ref().into_inner().commit().await?;
            local_transaction.commit().await?;
        } else {
            local_transaction.rollback().await?;
            //my_other.as_ref().into_inner().rollback().await?;
        }
    } else {
        let local_transaction = transaction.unwrap(); //Rc::new( RefCell::new( transaction.unwrap() ) );

        let mut rows = sqlx::query(query).fetch(&mut **local_transaction);

        process_data(&mut rows);

        /*
        loop {
            let row = match rows.try_next().await {
                Ok(optional) => match optional {
                    Some(row) => row,
                    None => {
                        break; //No more records.
                    }
                },
                Err(error) => {
                    local_error = Some(error);
                    break;
                }
            };

            ////let mut data: HashMap<String, serde_json::Value> = HashMap::new();

            // for column in row.columns().iter() {
            //     let ordinal = column.ordinal();
            //     let type_name = column.type_info().name();

            //     data.insert(
            //         column.name().to_string(),
            //         match type_name {
            //             "TEXT" | "VARCHAR" | "JSON" | "CHAR" => {
            //                 json!(row.get::<Option<String>, _>(ordinal))
            //             }
            //             "INTEGER" => json!(row.get::<Option<i64>, _>(ordinal)),
            //             "BOOLEAN" => json!(row.get::<Option<bool>, _>(ordinal)),
            //             "REAL" => json!(row.get::<Option<f64>, _>(ordinal)),
            //             // probably missed a few other types?
            //             _ => {
            //                 json!(format!("UNPROCESSED TYPE '{}'", type_name))
            //             }
            //         },
            //     );
            // }

            let data = row
                .columns()
                .into_iter()
                .map(|column| {
                    //row.get
                    let ordinal = column.ordinal();
                    let type_name = column.type_info().name();
                    (
                        column.name().to_string(),
                        match type_name {
                            "TEXT" | "VARCHAR" | "JSON" | "CHAR" => {
                                json!(row.get::<Option<String>, _>(ordinal))
                            }
                            "INTEGER" => json!(row.get::<Option<i64>, _>(ordinal)),
                            "BOOLEAN" => json!(row.get::<Option<bool>, _>(ordinal)),
                            "REAL" => json!(row.get::<Option<f64>, _>(ordinal)),
                            // probably missed a few other types?
                            _ => {
                                json!(format!("UNPROCESSED TYPE '{}'", type_name))
                            }
                        },
                    )
                        .to_owned()
                })
                .collect::<HashMap<_, _>>();

            result_data.push(data);
            //println!("{}", serde_json::to_string_pretty(&data).unwrap());
        }
        */
    }

    //current_transaction.commit();

    if local_error.is_some() {
        return Err(local_error.unwrap());
    }

    Ok(result_data)
}

fn main() {
    println!("My pid is {}", process::id());

    let mut line = String::new();
    println!("1. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let connection_pool = task::block_on(do_test_connection());

    println!(
        "2. Connections {}",
        connection_pool.as_ref().unwrap().size()
    );
    println!(
        "2. Active and idle connections {}",
        connection_pool.as_ref().unwrap().num_idle()
    );
    println!(
        "2. Connection options {:?}",
        connection_pool.as_ref().unwrap().connect_options()
    );
    println!(
        "2. Pool options {:?}",
        connection_pool.as_ref().unwrap().options()
    );
    println!("2. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let mut conn = match task::block_on(connection_pool.as_ref().unwrap().acquire()) {
        Ok(conn) => conn,
        Err(error) => {
            println!("Error: {}", error);
            return;
        }
    };

    let result = task::block_on(do_query_07(
        Some(&mut conn),
        None,
        "Select * From sysUserGroup",
    ));

    if result.is_ok() {
        println!(
            "{}",
            serde_json::to_string_pretty(&result.unwrap()).unwrap()
        );
    }

    println!(
        "3. Active and idle connections {}",
        connection_pool.as_ref().unwrap().num_idle()
    );
    println!("3. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let mut trans = match task::block_on(conn.begin()) {
        Ok(trans) => trans,
        Err(error) => {
            println!("Error: {}", error);
            return;
        }
    };

    let result = task::block_on(do_query_07(
        None,
        Some(&mut trans),
        "Select * From sysUserGroup",
    ));

    if result.is_ok() {
        let _ = task::block_on(trans.commit());
        println!(
            "{}",
            serde_json::to_string_pretty(&result.unwrap()).unwrap()
        );
    }

    println!("4. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let _ = task::block_on(do_query_06(&connection_pool, "Select * From sysUserGroup"));

    println!("5. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let _ = task::block_on(do_query_05(
        &connection_pool,
        "Select * From sysUserGroup Where Id = '040de405-872f-4a4f-a085-c494a76ed03b'",
    ));

    println!("6. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let _ = task::block_on(do_query_01(&connection_pool, "Select * From sysUserGroup"));

    println!("7. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let _ = task::block_on(do_query_02(&connection_pool, "Select * From sysUserGroup"));

    println!("8. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let _ = task::block_on(do_query_03(&connection_pool, "Select * From sysUserGroup"));

    println!("9. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    let _ = task::block_on(do_query_04(&connection_pool, "Select * From sysUserGroup1"));

    println!("10. Press any key to continue");
    std::io::stdin()
        .read_line(&mut line)
        .expect("Failed to read line");

    // match connection_pool {
    //     Some(connection_pool) => {}
    //     None => {
    //         println!("Cannot connect to database.");
    //     }
    // }
}
