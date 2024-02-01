import argparse
import pprint
from dataclasses import dataclass
from typing import Optional

import arc_endpoints
import dist_ref_id
import jmespath
import requests


@dataclass
class MigrationJson:
    ANS: dict
    arcAdditionalProperties: dict


@dataclass
class DocumentReferences:
    images: Optional[list] = None
    distributor: Optional[dict] = None


class Arc2SandboxGallery:
    """
    Usage: Copy one Gallery via its arc id from source organization production environment into its sandbox environment.
    - The script sets up a class where an ETL process takes place
    - CLass properties are modified by class methods, resulting in the transformed ANS.
    - There is a class method to extract an object's data from Arc, several class methods to apply other transformations,
    a class method to validate the transformed ANS, and a class method to load transformed ANS into a target organization.
    - Start by looking at the doit() method at the bottom of the script.

    Results:
    - Gallery will exist in target organization's sandbox environment.
    - References used in this gallery will be cataloged and a list of them returned.
    - Image references in the gallery will retain the same image ids.
    They will not go through the re-id process as in 05_transform_gallery.py because they stay in the same organization.
    - Script will circulate the gallery to the same website and sections as used in production
    - Distributor property in the ANS will be written to use the sandbox distributor id if it has been created in the sandbox environment.
    - Script will attempt to create sandbox distributor based off of the original one from production, to be used in the sandbox version of the ANS.

    Example terminal usage:
    python this_script.py --from-org devtraining  --gallery-arc_id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token --to-token devtraining sandbox token  --dry-run 1

    :modifies:
        self.references: {}
        self.ans: {}
        self.message: ""

    """
    def __init__(self, arc_id, from_org, to_org, source_auth, target_auth, dry_run):
        self.dry_run = bool(int(dry_run))
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.gallery_arc_id = arc_id
        self.ans = {}
        self.references = DocumentReferences()
        self.validation = None
        self.message = ""
        self.dry_run_restriction_msg = (
            "new distributors are not created during a dry run"
        )

    def fetch_source_ans(self):
        """
        :modifies:
            self.ans
            self.message
        """
        if self.dry_run:
            print(
                "THIS IS A TEST RUN. NEW GALLERY WILL NOT BE CREATED. NEW DISTRIBUTORS AND RESTRICTIONS WILL NOT BE CREATED."
            )

        gallery_res = requests.get(
            arc_endpoints.get_galleries_url(self.from_org, self.gallery_arc_id),
            headers=self.arc_auth_header_source,
        )
        if gallery_res.ok:
            self.ans = gallery_res.json()
        else:
            self.message = (
                f"{gallery_res} {self.from_org} {self.gallery_arc_id} {gallery_res.text}"
            )

    def transform_ans(self):
        """
        removes properties necessary to allow object to be ingested into new org
        sets properties with values appropriate to target org
        sets version to specific ANS version
        sets an additional_properties value to track that the object in the target org originated from the source org

        :modifies:
            self.ans
        """
        self.ans.get("owner", {}).update({"id": self.to_org})
        self.ans["version"] = "0.10.9"
        self.ans["additional_properties"].pop("version", None)
        self.ans["additional_properties"][
            "ingestionMethod"
        ] = f"copied from production {self.from_org} to {self.to_org}"

    def transform_promo_item(self):
        """
        Galleries don't retain reference syntax when fetched from the API, but the reference syntax is necessary to ingest a new gallery object.
        Reformat the image in `promo_items` as a reference.
        :modifies:
            self.ans
        """
        if jmespath.search("promo_items.basic._id", self.ans):
            old_id = self.ans["promo_items"]["basic"]["_id"]
            self.ans["promo_items"]["basic"] = {
                "_id": old_id,
                "type": "reference",
                "referent": {"type": "image", "id": old_id},
            }

    def transform_content_elements(self):
        """
        Galleries don't retain reference syntax when fetched from the API, but the reference syntax is necessary to ingest a new gallery object.
        Reformat the images in `content_elements` as references
        :modifies:
            self.references
            self.ans
        """
        ce_imgs = self.ans["content_elements"]
        self.references.images = jmespath.search("[*]._id", ce_imgs)
        for index, element in enumerate(ce_imgs):
            element = {
                "type": "reference",
                "_id": element["_id"],
                "referent": {"id": element["_id"], "type": "image"},
            }
            ce_imgs[index] = element

    def transform_distributor(self):
        """
        Figure out what the new distributor id for sandbox should be, update in ANS
        If no sandbox distributor already exists, script will attempt to create distributor and its restrictions
        If create of sandbox distributor does not work, the distributor.reference_id in story ans will be set to None
        and story ANS will fail validation
        If ANS fails validation because of a None distributor, create the sandbox distributor first with same details as source,
        and come back to this script and transform ANS

        :modifies:
            self.references
            self.ans
        """
        if not self.dry_run:
            (
                self.ans,
                references_distributor,
            ) = dist_ref_id.create_target_distributor_restrictions(
                self.from_org,
                self.to_org,
                self.ans,
                self.arc_auth_header_source,
                self.arc_auth_header_target,
                self.ans["canonical_website"],
            )
            self.references.distributor = references_distributor
            self.references.distributor.update(
                {"production": "sandbox"}
            ) if references_distributor else None

        if jmespath.search("distributor.reference_id", self.ans):
            orig_dist_id = self.ans["distributor"]["reference_id"]
            if self.dry_run:
                self.ans["distributor"]["reference_id"] = self.dry_run_restriction_msg
                self.references.distributor = {
                    "production": "sandbox",
                    orig_dist_id: self.dry_run_restriction_msg,
                }
            else:
                self.ans["distributor"]["reference_id"] = references_distributor.get(
                    orig_dist_id, None
                )

    def validate_transform(self):
        gallery_res2 = requests.post(
            arc_endpoints.ans_validation_url(self.to_org),
            headers=self.arc_auth_header_target,
            json=self.ans,
        )
        if gallery_res2.ok:
            self.validation = True
        else:
            self.validation = False
            self.message = f"{gallery_res2} {gallery_res2.text}"
        print("gallery validation", self.validation, self.gallery_arc_id)

    def post_transformed_ans(self):
        # post transformed ans to new organization
        mc = MigrationJson(self.ans, {})
        gallery_res3 = requests.post(
            arc_endpoints.mc_create_ans_url(self.to_org),
            headers=self.arc_auth_header_target,
            json=mc.__dict__,
            params={"ansId": self.gallery_arc_id, "ansType": "gallery"},
        )
        print("ans posted to sandbox MC", gallery_res3)

    def document_redirects(self):
        pass

    def doit(self):
        self.fetch_source_ans()
        if not self.ans:
            return self.message, None
        self.transform_ans()
        self.transform_content_elements()
        self.transform_promo_item()
        self.transform_distributor()
        self.validate_transform()
        if not self.validation:
            return self.message, None
        elif not self.dry_run:
            self.post_transformed_ans()
        return self.references.__dict__, self.ans


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-org",
        dest="org",
        required=True,
        default="",
        help="production organization id. the to-org is automatically the sandbox version of this value.",
    )
    parser.add_argument(
        "--from-token",
        dest="from_token",
        required=True,
        default="",
        help="production environment organization bearer token",
    )
    parser.add_argument(
        "--to-token",
        dest="to_token",
        required=True,
        default="",
        help="sandbox environment organization bearer token",
    )
    parser.add_argument(
        "--gallery-arc-id",
        dest="gallery_arc_id",
        required=True,
        default="",
        help="arc id value of gallery to migrate into sandbox environment",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        required=False,
        default=0,
        help="Set this to 1 to test the results of transforming an object. The object will not actually post to the target org.",
    )
    args = parser.parse_args()

    arc_auth_header_source = {"Authorization": f"Bearer {args.from_token}"}
    arc_auth_header_target = {"Authorization": f"Bearer {args.to_token}"}

    result = Arc2SandboxGallery(
        arc_id=args.gallery_arc_id,
        from_org=args.org,
        to_org=f"sandbox.{args.org}",
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        dry_run=args.dry_run,
    ).doit()
    pprint.pp(result)
