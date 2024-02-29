use sqlx::{Error, MySql, Pool}; //, MySqlPool

use sqlx::mysql::MySqlPoolOptions;

use async_std::task;

use futures::TryStreamExt;

use sqlx::Row;

use std::process;

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

fn main() {

    println!("My pid is {}", process::id());

    let mut line = String::new();
    println!( "Press any key to continue" );
    std::io::stdin().read_line(&mut line).expect("Failed to read line");

    let connection_pool = task::block_on(do_test_connection());

    println!( "Press any key to continue" );
    std::io::stdin().read_line(&mut line).expect("Failed to read line");

    let _ = task::block_on(do_query_01(&connection_pool, "Select * From sysUserGroup"));

    println!( "Press any key to continue" );
    std::io::stdin().read_line(&mut line).expect("Failed to read line");

    let _ = task::block_on(do_query_02(&connection_pool, "Select * From sysUserGroup"));

    println!( "Press any key to continue" );
    std::io::stdin().read_line(&mut line).expect("Failed to read line");

    let _ = task::block_on(do_query_03(&connection_pool, "Select * From sysUserGroup"));

    println!( "Press any key to continue" );
    std::io::stdin().read_line(&mut line).expect("Failed to read line");

    // match connection_pool {
    //     Some(connection_pool) => {}
    //     None => {
    //         println!("Cannot connect to database.");
    //     }
    // }
}
