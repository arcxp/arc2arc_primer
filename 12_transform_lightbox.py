import argparse
import pprint
from dataclasses import dataclass
from typing import Optional

import arc_endpoints
import arc_id
import requests
import jmespath


@dataclass
class DocumentReferences:
    images: Optional[dict] = None


class Arc2ArcLightbox:
    """
    Usage: Copy one lightbox via its id from source organization (production environment) into a target organization
    (production environment).
    - Lightbox JSON is not ANS

    Results:
    - Lightbox will exist in target organization's production environment.
    - References to Images that need to be created will be cataloged and a list of them are returned.
    - The Images behind the references will not be moved into the target organization.
    - Image references are reconstructed to use new image ids.
    This re-id is necessary when moving Photo Center objects to a new org.
    - Does not cause the image objects in the self.references to be ingested to the target organization.
    - The returned catalog of references self.references can be used to inform additional operations
    that might be necessary to bring the referenced objects into the target organization.

    Example terminal usage:
    python this_script.py --from-org devtraining --to-org cetest --lightbox-id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token --to-token cetest prod token  --dry-run 1

    :modifies:
        self.lightbox: {}
        self.references: {}
        self.message: ""
    """
    def __init__(
        self, lightbox_id, from_org, to_org, source_auth, target_auth, dry_run
    ):
        self.dry_run = bool(int(dry_run))
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.lightbox_id = lightbox_id
        self.lightbox_images_newids = []
        self.lightbox = {}
        self.references = DocumentReferences()
        self.message = ""

    def fetch_source_lightbox(self):
        """
        :modifies:
            self.lightbox
            self.message
        """
        if self.dry_run:
            print("THIS IS A TEST RUN. NEW LIGHTBOX WILL NOT BE CREATED.")

        lightbox_res = requests.get(
            arc_endpoints.get_lightbox_url(self.from_org, self.lightbox_id),
            headers=self.arc_auth_header_source,
        )
        if lightbox_res.ok:
            self.lightbox = lightbox_res.json()
        else:
            self.message = (
                f"{lightbox_res} {self.from_org} {self.lightbox_id} {lightbox_res.text}"
            )

    def transform_lightbox(self):
        """
        Remove fields from source lightbox JSON to allow lightbox to be created in target org
        :modifies:
            self.lightbox
        """
        self.lightbox.pop("id", None)
        self.lightbox.pop("created_date", None)

    def transform_lightbox_photos(self):
        """
        Finds all images in the lightbox and rewrites the references using image arc ids that can be used in the target organization
        :modifies:
            self.lightbox
            self.references
            self.message
        """
        images = jmespath.search("photos[*]._id", self.lightbox) or []
        if images:
            self.references.images = {self.from_org: self.to_org}
            for image in images:
                new_id = arc_id.generate_arc_id(image, self.to_org)
                self.references.images.update({image: new_id})
                self.lightbox_images_newids.append(new_id)
        else:
            self.message = f"There are no photos from {self.from_org} {self.lightbox_id} to put in a new lightbox. Process finished."

    def post_transformed_lightbox(self):
        # post lightbox to new organization
        try:
            lightbox_res2 = requests.post(
                arc_endpoints.get_lightbox_url(self.to_org),
                headers=self.arc_auth_header_target,
                json=self.lightbox,
            )
        except Exception as e:
            self.message = f"{lightbox_res2.status_code} {lightbox_res2.reason} {str(e)}"

        else:
            print("lightbox posted to new org", lightbox_res2)
            if lightbox_res2.ok:
                new_lightbox_id = lightbox_res2.json().get("id")
                lightbox_res3 = requests.post(
                    arc_endpoints.get_lightbox_url(self.to_org, new_lightbox_id, True),
                    headers=self.arc_auth_header_target,
                    json=self.lightbox_images_newids,
                )
                print("photos posted to new lightbox in new org", lightbox_res3)
            else:
                self.message = f"{lightbox_res2} {self.from_org} {self.lightbox_id} {lightbox_res2.text}"

    def doit(self):
        self.fetch_source_lightbox()
        if self.message:
            return self.message
        self.transform_lightbox()
        self.transform_lightbox_photos()
        if self.message:
            return self.message
        if not self.dry_run:
            self.post_transformed_lightbox()
            if self.message:
                print(self.message)
        return self.references.__dict__


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-org",
        dest="org",
        help="source organization id value; org for production or sandbox.org for sandbox",
        required=True,
        default="",
    )
    parser.add_argument(
        "--to-org",
        dest="to_org",
        help="target organization id value; org for production or sandbox.org for sandbox",
        required=True,
        default="",
    )
    parser.add_argument(
        "--from-token",
        dest="from_token",
        help="source organization bearer token; production environment",
        required=True,
        default="",
    )
    parser.add_argument(
        "--to-token",
        dest="to_token",
        help="target organization bearer token; production environment",
        required=True,
        default="",
    )
    parser.add_argument(
        "--lightbox-id",
        dest="lightbox_id",
        help="lightbox id to migrate into target org",
        required=True,
        default="",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        help="Set this to 1 to test the results of transforming a lightbox. The lightbox will not actually post to the target org.",
        required=False,
        default=1,
    )
    args = parser.parse_args()

    arc_auth_header_source = {"Authorization": f"Bearer {args.from_token}"}
    arc_auth_header_target = {"Authorization": f"Bearer {args.to_token}"}

    result = Arc2ArcLightbox(
        lightbox_id=args.lightbox_id,
        from_org=args.org,
        to_org=args.to_org,
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        dry_run=args.dry_run,
    ).doit()
    print('\nRESULTS')
    pprint.pp(result)
