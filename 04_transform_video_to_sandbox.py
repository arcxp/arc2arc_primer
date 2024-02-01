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
    distributor: Optional[dict] = None


class Arc2SandboxVideo:
    """
    Usage: Copy one Video via its arc id from an organization's production environment to the sandbox environment
    The script models the simplest transformation of Video and its circulation.
    - The script sets up a class where an ETL process takes place
    - CLass properties are modified by class methods, resulting in the transformed ANS.
    - There is a class method to extract an object's data from Arc, several class methods to apply other transformations,
    a class method to validate the transformed ANS, and a class method to load transformed ANS into a target organization.
    - Start by looking at the doit() method at the bottom of the script.

    Results:
    - Video will exist in target organization's sandbox environment.
    - Script will not encode videos. This can be changed by modifying the Migration Center JSON properties.
    - Script does not change video circulations. Script will circulate video in sandbox to same places it was circulated in production
    - Video promo images will be imported into sandbox.
    - Distributors in the Video ANS will be written to use sandbox distributor ids if they have been created in the  sandbox environment.
    - Script will attempt to create sandbox Distributors if necessary.
    - Geographic Restrictions in the Video ANS will be written to use the sandbox restriction ids if they have been created in sandbox.
    - Script will attempt to create sandbox Geographic Restrictions if necessary.
    - Script will not create Video redirects in sandbox.
    There's no way to get a list of redirects attached to a video, without already knowing the specific redirect url.
    Instead video redirects will have to be recreated using a script where the capi is queried specifically for redirect objects.
    See 11_transform_redirects_all.py

    Example terminal usage:
    python this_script.py --from-org devtraining --video-arc_id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token  --to-token devtraining sandbox token   --dry-run 1

    :modifies:
        self.references: {}
        self.ans: {}
        self.message: ""
    """
    def __init__(self, arc_id, from_org, to_org, source_auth, target_auth, dry_run):
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.dry_run = bool(int(dry_run))
        self.from_org = from_org
        self.to_org = to_org
        self.video_arc_id = arc_id
        self.ans = {}
        self.references = DocumentReferences()
        self.validation = None
        self.message = ""
        self.dry_run_restriction_msg = "new distributors and geo restrictions not created during a dry run"

    def fetch_source_ans(self):
        """
        Will not return source ANS if target object already exists unless --dry-run 1, because
           - there is a slightly different process to update a video than to create a new one
           - if you were transcoding videos, that process would incur a cost, so you shouldn't do so unnecessarily
           - you don't want to create duplicates of the video's promo_item image by recreating the same video multiple times
        You can see what the target ANS of an object looks like without creating it by passing in script parameter --dry_run 1
            - this includes objects already created in the target org

        :modifies:
            self.ans
            self.message
        """
        if self.dry_run:
            print(
                "THIS IS A TEST RUN. NEW VIDEO WILL NOT BE CREATED. NEW DISTRIBUTORS AND RESTRICTIONS WILL NOT BE CREATED."
            )

        video_exists_res = requests.get(
            arc_endpoints.get_video_url(self.from_org, "sandbox"),
            headers=self.arc_auth_header_target,
            params={"uuid": self.video_arc_id},
        )
        if not self.dry_run and video_exists_res.ok and video_exists_res.json():
            self.message = f"video {self.video_arc_id} already exists on sandbox, do not migrate {video_exists_res}"
        else:
            #  Retrieve source organization's video ANS content
            video_res = requests.get(
                arc_endpoints.get_video_url(self.from_org, "prod"),
                headers=self.arc_auth_header_source,
                params={"uuid": self.video_arc_id},
            )
            if video_res.ok:
                self.ans = video_res.json()[0]
            elif video_res.status_code == 404:
                self.message = f"{video_res} {self.from_org} {self.video_arc_id} is not a published video"
            else:
                self.message = (
                    f" {video_res} {self.from_org} {self.video_arc_id} {video_res.text}"
                )

    def transform_ans(self):
        """
        removes properties necessary to allow object to be ingested into new org
        sets properties with values appropriate to target org
        sets version to specific ANS version.  Only 0.8.0 ANS version is correct for videos.
        sets an additional_properties value to track that the object in the target org originated from the source org

        :modifies:
            self.ans
        """
        self.ans["_id"] = self.video_arc_id
        self.ans.get("owner", {}).update({"id": self.to_org})
        self.ans["version"] = "0.8.0"
        self.ans.pop("embed_html", None)
        self.ans.get("source", {}).pop("edit_url", None)
        self.ans["additional_properties"][
            "ingestionMethod"
        ] = f"copied from production {self.from_org} to {self.to_org}"

    def transform_promo_item(self):
        """
        Video `promo_items` don't use normal reference syntax

        rebuild `promo_items` ANS, causing the image to be imported into the target org
        remove the original promo item's anglerfish/photo center ans id from the ANS in `additional_properties`
            anglerfish_id exists in additional_properties when a user has manually created a thumbnail from a video using the UI
            and also checked a box in the UI to save the thumbnail to photo center. When you create the thumbnail with the PC API,
            the image is not also added to Photo Center (this is a bug that is on the roadmap to be fixed).
            Since maintaining anglerfish_id when creating an image with the API is meaningless, remove it to avoid confusion and extra work

        :modifies:
            self.ans
        """
        try:
            anglerfish = self.ans["additional_properties"]["anglerfisharc_id"]
            self.ans["additional_properties"].pop(anglerfish)
            self.ans["additional_properties"].pop("anglerfisharc_id")
        except:
            pass

        # promo image/promo item; rebuild the promo item ANS, causing the image to be imported into the new org
        if self.ans.get("promo_image").get("url"):
            self.ans["promo_items"] = {
                "basic": {
                    "type": "image",
                    "url": self.ans["promo_image"]["url"],
                    "version": "0.8.0",
                }
            }
            self.ans.pop("promo_image", None)
        else:
            self.ans.pop("promo_items", None)
            self.ans.pop("promo_image", None)

    def transform_circulation(self):
        """
        Videos don't retain reference syntax when fetched from the API, but the reference syntax is necessary to ingest a new gallery object.

        reformat `taxonomy.primary_section`, `taxonomy.section` property to use references
        reformat `websites` property
        circulates to the same sections on sandbox as original sections on production

        :modifies:
            self.ans
        """
        # reformat taxonomy.primary_section, sections to use references
        self.ans["taxonomy"].pop("primary_site", None)
        self.ans["taxonomy"].pop("sites", None)
        section = self.ans["taxonomy"]["primary_section"]["_id"]
        website = self.ans["taxonomy"]["primary_section"]["_website"]
        section_reference = {
            "type": "reference",
            "referent": {
                "id": section,
                "type": "section",
                "website": website,
                "referent_properties": {"additional_properties": {"primary": True}},
            },
        }
        self.ans["taxonomy"]["primary_section"] = section_reference
        for i, s in enumerate(self.ans["taxonomy"]["sections"]):
            section_reference["referent"]["id"] = s["_id"]
            section_reference["referent"]["website"] = s["_website"]
            if s.get("primary", False):
                section_reference["referent"]["referent_properties"] = {
                    "additional_properties": {"primary": True}
                }
            else:
                section_reference["referent"].pop("referent_properties", None)
            self.ans["taxonomy"]["sections"][i] = section_reference

        # reformat websites to remove site data
        for w in self.ans["websites"]:
            self.ans["websites"][w].pop("website_section", None)

    def transform_distributor(self):
        """
        Figure out what the new distributor id for sandbox should be, update in ANS
        If distributor does not already exist in sandbox, script will attempt to create distributor and its restrictions
        If create of sandbox distributor does not work, the distributor.reference_id in story ans will be set to None
        and story ANS will fail validation
        If ANS fails validation because of a None distributor, create the sandbox distributor first with same details as source,
        and come back to this script and transform ANS

        :return:
            self.document_references
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
            # just for videos, when there's a distributor, there's also an illegal entry in credits.affiliation that will break ANS validation
            self.ans["credits"].pop("affiliation", None)

    def transform_geographic_restriction(self):
        """
        Figure out what the new geographic restriction id for target org should be, update in ANS
        If geographic restriction does not already exist in target org, script will attempt to create it

        :modifies:
            self.ans
            self.references
        """
        (
            self.ans,
            geo_restrictions,
        ) = dist_ref_id.create_target_geographic_restrictions(
            self.from_org,
            self.to_org,
            self.ans,
            self.arc_auth_header_source,
            self.arc_auth_header_target,
            self.dry_run_restriction_msg,
            self.dry_run,
        )
        if geo_restrictions:
            self.references.geo_restrictions = geo_restrictions
            self.references.geo_restrictions.update(
                {self.from_org: self.to_org}
            )

    def other_supporting_references(self):
        """
        adds `related_content` objects to document references
        script does not create redirects
            - redirects attached to the Video are possible, but they are not represented in the Video ANS directly
            - it is not possible to discover the video redirects using a video's arc id or video canonical url
            - to find video redirects you must query content api `type: redirect` and then run a 2nd query using the
                url returned from 1st query to determine if it is for a video
            - see 11_tranform_redirects_all.py

        :modifies:
            self.references
        """
        # related_content, but remove if malformed because will fail the ANS validation
        if self.ans.get("related_content", {}).get("basic"):
            if not jmespath.search("related_content.basic[*]._id", self.ans):
                self.ans["related_content"]["basic"] = []
            else:
                self.references.related_content = jmespath.search(
                    "related_content.basic[*].{id: _id, type: referent.type}",
                    self.ans,
                    jmespath.Options(dict_cls=dict),
                )

    def validate_transform(self):
        video_res2 = requests.post(
            arc_endpoints.ans_validation_url(self.to_org, "0.8.0"),
            headers=self.arc_auth_header_target,
            json=self.ans,
        )
        if video_res2.ok:
            self.validation = True
        else:
            self.validation = False
            self.message = f"{video_res2} {video_res2.text}"
        print("video validation", self.validation, self.video_arc_id)

    def post_transformed_ans(self):
        if not self.dry_run:
            # post transformed ans to sandbox
            mc = MigrationJson(
                self.ans, {"video": {"transcoding": False, "useLastUpdated": True}}
            )
            video_res3 = requests.post(
                arc_endpoints.mc_create_ans_url(self.to_org),
                headers=self.arc_auth_header_target,
                json=mc.__dict__,
                params={"ansId": self.video_arc_id, "ansType": "video"},
            )
            print("ans posted to sandbox MC", video_res3)

    def doit(self):
        self.fetch_source_ans()
        if not self.ans:
            return self.message, None
        self.transform_ans()
        self.transform_circulation()
        self.transform_promo_item()
        self.transform_distributor()
        self.transform_geographic_restriction()
        self.other_supporting_references()
        self.validate_transform()
        if not self.validation:
            return self.message, None
        else:
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
        "--video-arc-id",
        dest="video_arc_id",
        required=True,
        default="",
        help="arc id value of video to migrate into sandbox environment",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        required=False,
        default=0,
        help="A video that exists in the target org will not be processed.  Set this to 1 to process the video enough to see the transformed ANS.  However, the video will not actually post to the target org.",
    )
    args = parser.parse_args()

    arc_auth_header_source = {"Authorization": f"Bearer {args.from_token}"}
    arc_auth_header_target = {"Authorization": f"Bearer {args.to_token}"}

    result = Arc2SandboxVideo(
        arc_id=args.video_arc_id,
        from_org=args.org,
        to_org=f"sandbox.{args.org}",
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        dry_run=args.dry_run,
    ).doit()
    pprint.pp(result)
