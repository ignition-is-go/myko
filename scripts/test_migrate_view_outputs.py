"""Run with python -m unittest discover -s scripts -p test_migrate_view_outputs.py."""

import unittest

from migrate_view_outputs import migrate


class ViewOutputMigrationTests(unittest.TestCase):
    def test_local_plan_is_wrapped_once(self):
        source = '''impl ViewHandler for Example {
    fn build_cell(context: ViewBuildArgs<Self>)
        -> impl hyphae::MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
        context.items().filter_map_entries(|key, item| Some((key, item)))
    }
}'''
        migrated, count = migrate(source, "myko::view")
        self.assertEqual(count, 1)
        self.assertIn("impl myko::view::ViewBuildOutput<Item = Self::Item>", migrated)
        self.assertIn("myko::view::LocalView::new({", migrated)
        self.assertIn("context.items().filter_map_entries(|key, item| Some((key, item)))", migrated)
        self.assertEqual(migrate(migrated, "myko::view"), (migrated, 0))

    def test_existing_retained_output_is_unchanged(self):
        source = '''fn build_cell(context: ViewBuildArgs<Self>)
            -> impl myko::view::ViewBuildOutput<Item = Self::Item> {
            myko::view::RetainedView::new(context.snapshots())
        }'''
        self.assertEqual(migrate(source, "myko::view"), (source, 0))

    def test_return_requires_manual_review(self):
        source = '''fn build_cell(context: ViewBuildArgs<Self>)
            -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
            context.items().filter_map_entries(|key, item| {
                if item.hidden { return None; }
                Some((key, item))
            })
        }'''
        with self.assertRaisesRegex(ValueError, "contains return"):
            migrate(source, "crate::view")

    def test_comments_and_strings_do_not_change_body_boundaries(self):
        source = '''fn build_cell(context: ViewBuildArgs<Self>)
            -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
            // return and unmatched { are comment text
            context.named("return {")
        }'''
        migrated, count = migrate(source, "crate::view")
        self.assertEqual(count, 1)
        self.assertIn('context.named("return {")', migrated)


if __name__ == "__main__":
    unittest.main()
