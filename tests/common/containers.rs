use testcontainers::{clients::Cli, images, Container, RunnableImage};

pub async fn start_containers(docker: &Cli) -> (u16, Container<'_, images::postgres::Postgres>) {
    let image = RunnableImage::from(images::postgres::Postgres::default()).with_tag("14-alpine");
    let postgres = docker.run(image);
    let db_port = postgres.get_host_port_ipv4(5432);
    println!("Postgres postgres started on {}", db_port);

    (db_port, postgres)
}
