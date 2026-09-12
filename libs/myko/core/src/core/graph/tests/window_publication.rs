use super::*;

#[derive(Clone, Copy)]
enum Source {
    Indexed,
    Map,
}

#[derive(Clone, Copy)]
enum Page {
    Offset,
    Cursor,
}

#[test]
fn pending_indexed_offset_keeps_the_selected_page_update() {
    assert_pending_page_update(Source::Indexed, Page::Offset);
}

#[test]
fn pending_indexed_cursor_keeps_the_selected_page_update() {
    assert_pending_page_update(Source::Indexed, Page::Cursor);
}

#[test]
fn pending_map_offset_keeps_the_selected_page_update() {
    assert_pending_page_update(Source::Map, Page::Offset);
}

#[test]
fn pending_map_cursor_keeps_the_selected_page_update() {
    assert_pending_page_update(Source::Map, Page::Cursor);
}

fn assert_pending_page_update(kind: Source, page: Page) {
    let _serial = crate::test_util::scheduler_test_serial();
    let context = context();
    let tag = Tag {
        name: "pending window".into(),
        id: TagId::from("pending-window-tag"),
    };
    let article_a = Article {
        title: "A".into(),
        id: ArticleId::from("pending-window-article-a"),
    };
    let article_b = Article {
        title: "B".into(),
        id: ArticleId::from("pending-window-article-b"),
    };
    context.set(&tag).expect("tag");
    context.set(&article_a).expect("article A");
    context.set(&article_b).expect("article B");
    let edge_a = ForwardIndexedAssignment {
        tag_id: tag.id.clone(),
        article_id: article_a.id.clone(),
        id: ForwardIndexedAssignmentId::from("a"),
    };
    let mut edge_b = ForwardIndexedAssignment {
        tag_id: tag.id.clone(),
        article_id: article_b.id.clone(),
        id: ForwardIndexedAssignmentId::from("b"),
    };
    context.batch_set(&[edge_a, edge_b.clone()]).expect("edges");
    let endpoint = <ConcreteEndpoint<Tag> as EndpointSpec>::erase(&tag.id).expect("endpoint");
    let graph = context.graph_index().expect("graph index");
    let initial_window = crate::wire::QueryWindow {
        offset: 0,
        limit: 1,
    };
    let source = match kind {
        Source::Indexed => graph
            .watch_window_at(
                ForwardIndexedAssignment::ENTITY_NAME_STATIC,
                EndPosition::A,
                &endpoint,
                initial_window,
            )
            .expect("window")
            .expect("indexed window"),
        Source::Map => crate::query::WindowedQuerySource::from_map(
            &context
                .registry
                .get_or_create(ForwardIndexedAssignment::ENTITY_NAME_STATIC)
                .as_ref()
                .clone()
                .lock(),
            initial_window,
        ),
    };
    assert_eq!(source.snapshots().get().entries[0].0.as_ref(), "a");

    hyphae::batch(|| {
        match page {
            Page::Offset => source.set_window(Some(crate::wire::QueryWindow {
                offset: 1,
                limit: 1,
            })),
            Page::Cursor => source.set_cursor_window(crate::wire::QueryCursorWindow::after("a", 1)),
        }
        edge_b.article_id = article_a.id.clone();
        context.set(&edge_b).expect("update newly selected edge");
    });

    let settled = source.snapshots().get();
    assert_eq!(settled.entries.len(), 1);
    assert_eq!(settled.entries[0].0.as_ref(), "b");
    let selected = settled.entries[0]
        .1
        .as_any()
        .downcast_ref::<ForwardIndexedAssignment>()
        .expect("typed selected edge");
    assert_eq!(selected.article_id, article_a.id);
}
