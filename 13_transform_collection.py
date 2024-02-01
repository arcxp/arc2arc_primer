import argparse
import pprint
from dataclasses import dataclass
from typing import Optional

import arc_endpoints
import requests
import jmespath

@dataclass
class DocumentReferences:
    stories: Optional[list] = None


class Arc2ArcCollection:
    """
    Usage: Copy one Collection via its arc id from source organization (production environment) into a
    target organization (production environment).

    Results:
    - Collection will exist in target organization's production environment.
    - References to Arc objects used in Story will be cataloged and a list of them are returned.
    - The objects behind the references will not be moved into the target organization.

    Example terminal usage:
    python this_script.py --from-org devtraining --to-org cetest --collection-arcid MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token --to-token cetest prod token --to-website-site cetest --to-website-section /test  --dry-run 1

    :modifies:
        self.references: {}
        self.ans: {}
        self.message: ""
    """

    def __init__(
        self,
        collection_arc_id,
        from_org,
        to_org,
        source_auth,
        target_auth,
        to_website,
        dry_run,
    ):
        self.dry_run = bool(int(dry_run))
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.to_website = to_website
        self.collection_arc_id = collection_arc_id
        self.collection = {}
        self.references = DocumentReferences()
        self.message = ""

    def fetch_source_collection(self):
        """
        :modifies:
            self.collection
            self.message
        """
        if self.dry_run:
            print("THIS IS A TEST RUN. NEW COLLECTION WILL NOT BE CREATED.")

        collection_res = requests.get(
            arc_endpoints.get_collection_url(self.from_org, self.collection_arc_id),
            headers=self.arc_auth_header_source,
        )
        if collection_res.ok:
            self.collection = collection_res.json().get("data")
        else:
            self.message = f"{collection_res} {self.from_org} {self.collection_arc_id} {collection_res.text}"

    def transform_collection(self):
        """
        Remove fields from source collection ANS to allow collection to be created in target org
        Update fields necessary to save collection to target org
        Build list of references used in collection
        :modifies:
            self.collection
            self.references
            self.message
        """
        self.collection.pop("id", None)
        self.collection.pop("published_revision", None)
        self.collection.pop("current_revision", None)
        self.collection["canonical_website"] = self.collection["document"][
            "canonical_website"
        ] = self.to_website
        self.references.stories = jmespath.search(
            "content_elements[*].referent.id", self.collection.get("document")
        )

        if not self.references.stories:
            self.message = f"There are no stories from {self.from_org} {self.collection_arc_id} to put in a new collection. Process finished."

    def post_transformed_collection(self):
        # post collection to new organization
        collection_res2 = requests.post(
            arc_endpoints.get_collection_url(self.to_org),
            headers=self.arc_auth_header_target,
            json=self.collection,
        )
        print("collection posted to new org", collection_res2)
        if not collection_res2.ok:
            self.message = f"{collection_res2} {self.from_org} {self.collection_arc_id} {collection_res2.text}"

    def doit(self):
        self.fetch_source_collection()
        if self.message:
            return self.message
        self.transform_collection()
        if self.message:
            return self.message
        if not self.dry_run:
            self.post_transformed_collection()
        return self.references.__dict__


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-org",
        dest="org",
        required=True,
        default="",
        help="source organization id value; org for production or sandbox.org for sandbox'",
    )
    parser.add_argument(
        "--to-org",
        dest="to_org",
        required=True,
        default="",
        help="target organization id value; org for production or sandbox.org for sandbox'",
    )
    parser.add_argument(
        "--from-token",
        dest="from_token",
        required=True,
        default="",
        help="source organization bearer token; production environment'",
    )
    parser.add_argument(
        "--to-token",
        dest="to_token",
        required=True,
        default="",
        help="target organization bearer token; production environment'",
    )
    parser.add_argument(
        "--to-website-site",
        dest="to_website",
        required=True,
        default="",
        help="target organization's website name'",
    )
    parser.add_argument(
        "--collection-arc-id",
        dest="collection_arc_id",
        required=True,
        default="",
        help="Collection id to migrate into target org",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        required=False,
        default=1,
        help="Set this to 1 to test the results of transforming a collection. The collection will not actually post to the target org.",
    )
    args = parser.parse_args()

    arc_auth_header_source = {"Authorization": f"Bearer {args.from_token}"}
    arc_auth_header_target = {"Authorization": f"Bearer {args.to_token}"}

    result = Arc2ArcCollection(
        collection_arc_id=args.collection_arc_id,
        from_org=args.org,
        to_org=args.to_org,
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        to_website=args.to_website,
        dry_run=args.dry_run,
    ).doit()
    pprint.pp(result)
