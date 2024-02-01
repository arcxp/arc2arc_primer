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
    circulations: list
    arcAdditionalProperties: dict


@dataclass
class DocumentReferences:
    redirects: Optional[list] = None
    distributor: Optional[dict] = None



class Arc2SandboxStory:
    """
    Usage: Copy one Story via its arc id from an organization's production environment to the sandbox environment
    The script is not for production use, it is to demonstrate the transformations needed to
    change ANS from production so that it can be ingested into sandbox.
    - The script sets up a class where an ETL process takes place
    - CLass properties are modified by class methods, resulting in the transformed ANS.
    - There is a class method to extract an object's data from Arc, several class method to apply other transformations,
    a class method to validate the transformed ANS, and a class method to load transformed ANS into a target organization.
    - Start by looking at the doit() method at the bottom of the script.

    Results:
    - Story will exist in target organization's sandbox environment.
    - Script returns only the document references for distributors, as these are the only ids that will be different
    between production and sandbox
    - No changes are made to the story circulations
    - No changes are made to photo center ans ids
    - The script will create document redirects for this story in sandbox, matching the redirects for this story in production.
    The redirects will be added to the references display that are returned when the script is completed.
    - When the script is complete it will display an object showing the document references and redirects that were associated
    with the source object, if those items are useful for verifying the completeness of this piece of content.

    Example terminal usage:
    python this_script.py --from-org devtraining --story-arc_id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token --to-token devtraining sandbox token  --dry-run 1

    :modifies:
        self.document_references
        self.ans
        self.circulation
        self.message
    """

    def __init__(self, arc_id, from_org, to_org, source_auth, target_auth, dry_run):
        self.dry_run = bool(int(dry_run))
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.story_arc_id = arc_id
        self.ans = {}
        self.circulation = []  # won't be transformed, no expected changes between prod and sandbox in the same org
        self.validation = None
        self.message = ""
        self.references = DocumentReferences()
        self.dry_run_restriction_msg = "new distributors not created during a dry run"

    def fetch_source_ans(self):
        """ Extract ANS from source organization, the production environment
        :modifies:
            self.ans
            self.circulation
            self.message
        """
        if self.dry_run:
            print(
                "THIS IS A TEST RUN. STORY WILL NOT BE CREATED OR UPDATED. NEW DISTRIBUTORS AND RESTRICTIONS WILL NOT BE CREATED."
            )

        # You've got the ans id of the source story.  Find the published revision and its ans content.
        story_res = requests.get(
            arc_endpoints.draft_find_revision_url(self.from_org, self.story_arc_id),
            headers=self.arc_auth_header_source,
        )
        if story_res.ok:
            story_revision = jmespath.search("draft_revision_id", story_res.json())
            story_res2 = requests.get(
                arc_endpoints.draft_get_story_url(
                    self.from_org, self.story_arc_id, story_revision
                ),
                headers=self.arc_auth_header_source,
            )
            if story_res2.ok:
                self.ans = jmespath.search("ans", story_res2.json())
                story_res3 = requests.get(
                    arc_endpoints.draft_get_circulations_url(
                        self.from_org, self.story_arc_id
                    ),
                    headers=arc_auth_header_source,
                )
                if story_res3.ok:
                    self.circulation = jmespath.search("circulations", story_res3.json())
                else:
                    self.message = f"{story_res3} {story_res3.text} "
            else:
                self.message = f"{story_res2} {story_res2.text}"
        else:
            self.message = f"{story_res} {story_res.text}"

    def transform_ans(self):
        """
        removes properties necessary to allow object to be ingested into sandbox
        sets properties with values appropriate to sandbox
        sets version to specific ANS version
        sets an additional_properties value to track that the object in the target org originated from the source org
        :modifies:
            self.ans
        """
        self.ans["_id"] = self.story_arc_id
        self.ans["version"] = "0.10.9"
        self.ans.get("owner", {}).update({"id": self.to_org})
        self.ans.pop("revision", None)
        self.ans["additional_properties"][
            "ingestionMethod"
        ] = f"copied from production {self.from_org} to {self.to_org}"

    def transform_distributor(self):
        """
        Figure out what the new distributor id for target org should be, update in ANS
        If no distributor already exists in sandbox, script will attempt to create distributor and its restrictions
        If create of target distributor does not work, the distributor.reference_id in story ans will be set to None
        and story ANS will fail validation
        If ANS fails validation because of a None distributor, create the target distributor manually or in a seperate
        process first with same details as source. Come back to run this script again and transform ANS.

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
            self.references.distributor.update({"production": "sandbox"}) if references_distributor else None

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
        story_res4 = requests.post(
            arc_endpoints.ans_validation_url(self.to_org),
            headers=self.arc_auth_header_target,
            json=self.ans,
        )
        if story_res4.ok:
            self.validation = True
        else:
            self.validation = False
            self.message = f"{story_res4} {story_res4.text}"
        print("story validation", self.validation, self.story_arc_id)

    def post_transformed_ans(self):
        mc = MigrationJson(self.ans, self.circulation, {"story": {"publish": True}})
        story_res5 = requests.post(
            arc_endpoints.mc_create_ans_url(self.to_org),
            headers=self.arc_auth_header_target,
            json=mc.__dict__,
            params={"ansId": self.story_arc_id, "ansType": "story"},
        )
        print("ans posted to sandbox MC", story_res5)

    def document_redirects(self):
        """
        Process document level redirects into sandbox
        """
        story_res6 = requests.get(
            arc_endpoints.get_story_redirects_url(
                self.from_org, self.story_arc_id, self.ans.get("canonical_website")
            ),
            headers=self.arc_auth_header_source,
        )
        if story_res6.ok:
            redirects = story_res6.json()["redirects"]
            self.references.redirects = redirects
            if not self.dry_run:
                for red_url in redirects:
                    try:
                        story_res7 = requests.post(
                            arc_endpoints.get_story_redirects_url(
                                self.to_org,
                                self.story_arc_id,
                                self.ans.get("canonical_website"),
                                red_url["website_url"],
                            ),
                            headers=self.arc_auth_header_target,
                            json={"document_id": self.story_arc_id},
                        )
                        print("redirect created", story_res7.json())
                    except Exception as e:
                        print("redirect not processed", red_url, e)

    def doit(self):
        self.fetch_source_ans()
        if not self.ans and not self.circulation:
            return self.message, None
        self.transform_ans()
        self.transform_distributor()
        self.validate_transform()
        if not self.validation:
            return self.message, None
        elif not self.dry_run:
            self.post_transformed_ans()
            self.document_redirects()
        return self.references.__dict__, self.ans, self.circulation


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-org",
        dest="org",
        required=True,
        default="",
        help="production organization id. the to-org is automatically set as 'sandbox.org'",
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
        "--story-arc-id",
        dest="story_arc_id",
        required=True,
        default="",
        help="arc id value of story to migrate into sandbox environment",
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

    result = Arc2SandboxStory(
        arc_id=args.story_arc_id,
        from_org=args.org,
        to_org=f"sandbox.{args.org}",
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        dry_run=args.dry_run,
    ).doit()
    pprint.pp(result)
