use insta::assert_snapshot;
use minijinja::{render, Environment};
use minijinja_contrib::globals::{cycler, joiner};

#[test]
fn test_cycler() {
    let mut env = Environment::new();
    env.add_function("cycler", cycler);

    assert_snapshot!(render!(in env, r"{% set c = cycler([1, 2]) -%}
next(): {{ c.next() }}
next(): {{ c.next() }}
next(): {{ c.next() }}
cycler: {{ c }}"), @r###"
    next(): 1
    next(): 2
    next(): 1
    cycler: Cycler { items: [1, 2], pos: 1 }
    "###);
}

#[test]
fn test_joiner() {
    let mut env = Environment::new();
    env.add_function("joiner", joiner);

    assert_snapshot!(render!(in env, r"{% set j = joiner() -%}
first: [{{ j() }}]
second: [{{ j() }}]
joiner: {{ j }}"), @r"
    first: []
    second: [, ]
    joiner: Joiner { sep: ', ', used: true }
    ");

    assert_snapshot!(render!(in env, r"{% set j = joiner('|') -%}
first: [{{ j() }}]
second: [{{ j() }}]
joiner: {{ j }}"), @r"
    first: []
    second: [|]
    joiner: Joiner { sep: '|', used: true }
    ");
}

#[test]
#[cfg(feature = "rand")]
#[cfg(target_pointer_width = "64")]
fn test_lispum() {
    // The small rng is pointer size specific.  Test on 64bit platforms only
    use minijinja_contrib::globals::lipsum;

    let mut env = Environment::new();
    env.add_function("lipsum", lipsum);

    assert_snapshot!(render!(in env, r"{% set RAND_SEED = 42 %}{{ lipsum(5) }}"), @r###"
    Facilisi accumsan class rutrum integer euismod gravida cras vsociis arcu lobortis sociosqu elementum lacus nulla. Leo imperdiet penatibus id quam malesuada pretium sociosqu scelerisque diam sociosqu penatibus imperdiet et nisl. Ante s vulputate nulla porta ssociis per gravida primis porta penatibus nostra congue dui.

    Ipsum cras integer magna ssociis etiam eu rutrum ac praesent ssociis primis nisl malesuada sociosqu. Senectus sem neque ridiculus aliquet duis nisl facilisis quam diam nibh ad eget. Rutrum mauris aliquam faucibus magna eu phasellus ssociis libero neque convallis magna. Ante aliquet proin montes nibh sociosqu vulputate auctor.

    Lacinia aliquam dictumst pellentesque nibh sociosqu sagittis leo ad dictum elementum sapien mi sociosqu. Et ssociis laoreet dolor egestas scelerisque potenti duis natoque ssociis feugiat. Proin luctus porta rhoncus quis phasellus netus non proin sociosqu nonummy ornare lacinia. Leo sociis inceptos cum leo non elit class sed sapien dictum diam mattis dapibus netus facilisis. Hendrerit montes aliquam ssociis ridiculus a cras sociosqu nisi ssociis curabitur.

    Justo nonummy pulvinar potenti in potenti at facilisi platea sagittis scelerisque quis sapien semper dictum in ipsum. Nunc nonummy ornare etiam elementum nullam curae eu nullam ad nascetur ssociis nullam mus. Nisi ssociis gravida dapibus non sociosqu laoreet adipiscing potenti ipsum parturient potenti mollis odio. Leo eget felis pretium libero consectetuer hymenaeos sociosqu ssociis in posuere.

    S commodo fames ridiculus luctus proin non aptent nullam mi eleifend consectetuer aliquam ad. Scelerisque nisl blandit sociis euismod curae semper nunc nec litora condimentum fames habitasse. Inceptos augue sociosqu hendrerit justo montes orci proin mus molestie id iaculis nostra lacus. Cum facilisis potenti facilisis nonummy sem.

    "###);

    assert_snapshot!(render!(in env, r"{% set RAND_SEED = 42 %}{{ lipsum(2, html=true) }}"), @r###"
    <p>Facilisi accumsan class rutrum integer euismod gravida cras vsociis arcu lobortis sociosqu elementum lacus nulla. Leo imperdiet penatibus id quam malesuada pretium sociosqu scelerisque diam sociosqu penatibus imperdiet et nisl. Ante s vulputate nulla porta ssociis per gravida primis porta penatibus nostra congue dui.</p>

    <p>Ipsum cras integer magna ssociis etiam eu rutrum ac praesent ssociis primis nisl malesuada sociosqu. Senectus sem neque ridiculus aliquet duis nisl facilisis quam diam nibh ad eget. Rutrum mauris aliquam faucibus magna eu phasellus ssociis libero neque convallis magna. Ante aliquet proin montes nibh sociosqu vulputate auctor.</p>

    "###);
}

#[test]
#[cfg(feature = "rand")]
fn test_randrange() {
    use minijinja_contrib::globals::randrange;

    let mut env = Environment::new();
    env.add_function("randrange", randrange);

    assert_snapshot!(render!(in env, r"{% set RAND_SEED = 42 %}{{ randrange(10) }}"), @"1");
    assert_snapshot!(render!(in env, r"{% set RAND_SEED = 42 %}{{ randrange(-50, 50) }}"), @"-20");
}
