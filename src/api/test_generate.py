"""Regression tests for the supported OpenAPI model subset."""

import unittest

from generate import property_type


class DictionaryPropertiesTest(unittest.TestCase):
    def test_typed_dictionary_values(self):
        for schema, expected in [
            (["type: string"], "String"),
            (["type: integer"], "i64"),
            (["$ref: '#/components/schemas/Service'"], "Service"),
            (["type: array", "items:", "  type: string"], "Vec<String>"),
            (["type: object", "additionalProperties:", "  type: boolean"],
             "std::collections::BTreeMap<String, bool>"),
        ]:
            with self.subTest(schema=schema):
                block = ["          type: object", "          additionalProperties:"]
                block.extend("            " + line for line in schema)
                block.append("          description: A typed dictionary")
                self.assertEqual(
                    property_type(block),
                    f"std::collections::BTreeMap<String, {expected}>",
                )

    def test_untyped_dictionary_is_not_silently_accepted(self):
        for declaration in ["additionalProperties: true", "additionalProperties: false"]:
            with self.subTest(declaration=declaration):
                with self.assertRaisesRegex(ValueError, "additionalProperties"):
                    property_type(["          type: object", "          " + declaration])


if __name__ == "__main__":
    unittest.main()
