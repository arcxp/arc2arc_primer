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
    authors: Optional[list] = None
    distributor: Optional[dict] = None


class Arc2SandboxImage:
    """
    Usage: Copy one Image via its arc id from organization's production environment into its sandbox environment.
    - The script sets up a class where an ETL process takes place
    - CLass properties are modified by class methods, resulting in the transformed ANS.
    - There is a class method to extract an object's data from Arc, several class methods to apply other transformations,
    a class method to validate the transformed ANS, and a class method to load transformed ANS into a target organization.
    - Start by looking at the doit() method at the bottom of the script.

    Results:
    - Image will exist in target organization's sandbox environment.
    - Image arc id remains the same.
    - Distributor property in the ANS will be written to use the sandbox distributor ids if they have been created in the sandbox environment.
    - Script will attempt to create sandbox Distributors.

    Example terminal usage:
    python this_script.py --from-org devtraining --image-arc_id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token --to-token devtraining sandbox token  --dry-run 1

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
        self.image_arc_id = arc_id
        self.ans = {}
        self.references = DocumentReferences()
        self.message = ""
        self.validation = None
        self.dry_run_restriction_msg = "new distributors not created during a dry run"

    def fetch_source_ans(self):
        """
        :modifies:
            self.ans
            self.message
        """
        if self.dry_run:
            print(
                "THIS IS A TEST RUN. NEW IMAGE WILL NOT BE CREATED. NEW DISTRIBUTORS AND RESTRICTIONS WILL NOT BE CREATED."
            )

        image_res = requests.get(
            arc_endpoints.get_photo_url(self.from_org, self.image_arc_id),
            headers=self.arc_auth_header_source,
        )
        if image_res.ok:
            self.ans = image_res.json()
        else:
            self.message = (
                f"{image_res} {self.from_org} {self.image_arc_id} {image_res.text}"
            )

    def transform_ans(self):
        """
        removes properties necessary to allow object to be ingested into new org / sandbox
        - some of these properties are valid if the photo center api is used to create an image, but not when the ANS is validated
        - since migration center api validates ANS, these properties cannot be contained in the ANS in this script
        sets properties with values appropriate to target org
        sets version to specific ANS version
        sets an additional_properties value to track that the object in the target org originated from the source org

        :modifies:
            self.ans
        """
        self.ans.get("owner", {}).update({"id": self.to_org})
        self.ans["version"] = "0.10.9"
        self.ans["additional_properties"].pop("version", None)
        self.ans["additional_properties"].pop("galleries", None)
        self.ans["additional_properties"][
            "ingestionMethod"
        ] = f"copied from production {self.from_org} to {self.to_org}"
        self.ans.pop("auth")
        self.ans.get("source", {}).pop("edit_url", None)
        # these can be added to the ANS by video center when you clip an image from a video for its thumbnail.  will cause validation failure.
        self.ans.pop("imageId", None)
        self.ans.pop("ingestImageToAnglerfish", None)
        self.photo_center_specific_properties(remove=True)

    def photo_center_specific_properties(self, remove=True, put_back=False):
        # these can be added when the Photo Center Api is used to create the image, but are not valid ANS fields.
        # will cause validation failure.  Will remove then and copy values temporarily, then put back after validation.
        if remove:
            if self.ans.get("usage_instructions"):
                self.ans["additional_properties"]["usage_instructions"] = self.ans.get(
                "usage_instructions")
            if self.ans.get("photographer"):
                self.ans["additional_properties"]["photographer"] = self.ans.get("photographer")
            if self.ans.get("creditIPTC"):
                self.ans["additional_properties"]["creditIPTC"] = self.ans.get("creditIPTC")
            self.ans.pop("usage_instructions", None)
            self.ans.pop("photographer", None)
            self.ans.pop("creditIPTC", None)

        if put_back:
            if self.ans["additional_properties"].get("usage_instructions"):
                self.ans["usage_instructions"] = self.ans["additional_properties"].get("usage_instructions")
            if self.ans["additional_properties"].get("photographer"):
                self.ans["photographer"] = self.ans["additional_properties"].get("photographer")
            if self.ans["additional_properties"].get("creditIPTC"):
                self.ans["creditIPTC"] = self.ans["additional_properties"].get("creditIPTC")
            self.ans["additional_properties"].pop("usage_instructions", None)
            self.ans["additional_properties"].pop("photographer", None)
            self.ans["additional_properties"].pop("creditIPTC", None)

    def other_supporting_references(self):
        """
        :modifies:
            self.references
        """
        # Are there author references in the ans? build list.
        self.references.authors = (
            jmespath.search("credits.by[*].referent.id", self.ans) or []
        )

    def transform_distributor(self):
        """
        Figure out what the new distributor id for sandbox should be, update in ANS
        If no sandbox distributor already exists, script will attempt to create sandbox distributor and its restrictions
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
                "",
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
                    orig_dist_id: self.dry_run_restriction_msg,
                    "production": "sandbox"
                }
            else:
                self.ans["distributor"]["reference_id"] = references_distributor.get(
                    orig_dist_id, None
                )

    def validate_transform(self):
        # Validate transformed ANS
        image_res2 = requests.post(
            arc_endpoints.ans_validation_url(self.to_org),
            headers=self.arc_auth_header_target,
            json=self.ans,
        )
        if image_res2.ok:
            self.validation = True
            self.photo_center_specific_properties(remove=False, put_back=True)
        else:
            self.message = f"{image_res2} {image_res2.text}"
            self.validation = False
        print("image validation", self.validation, self.image_arc_id)

    def post_transformed_ans(self):
        mc = MigrationJson(self.ans, {})
        image_res3 = requests.post(
            arc_endpoints.mc_create_ans_url(self.to_org),
            headers=arc_auth_header_target,
            json=mc.__dict__,
            params={"ansId": self.image_arc_id, "ansType": "image"},
        )
        print("image sent to sandbox MC", image_res3, image_res3.json())

    def doit(self):
        self.fetch_source_ans()
        if not self.ans:
            return self.message, None
        self.transform_ans()
        self.other_supporting_references()
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
        help="source organization id value; org for production or sandbox.org for sandbox'",
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
        "--image-arc-id",
        dest="image_arc_id",
        required=True,
        default="",
        help="arc id value of image to migrate into target org",
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

    result = Arc2SandboxImage(
        arc_id=args.image_arc_id,
        from_org=args.org,
        to_org=f"sandbox.{args.org}",
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        dry_run=args.dry_run,
    ).doit()
    pprint.pp(result)
