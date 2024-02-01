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
    images: Optional[dict] = None
    distributor: Optional[dict] = None
    related_content: Optional[list] = None
    geo_restrictions: Optional[dict] = None
    circulation: Optional[dict] = None


class Arc2ArcVideo:
    """
    Usage: Copy one Video via its arc id from source organization into a target organization (production environments).
    The script models the simplest transformation of Video and its circulation.  The script is not meant to cover all
    complex circumstances, such as a video that is circulated simultaneously to multiple websites and sections. These
    situations are left for the reader to determine the extensions and logic necessary to enable their unique and more
    complex situation.
    - The script sets up a class where an ETL process takes place
    - CLass properties are modified by class methods, resulting in the transformed ANS.
    - There is a class method to extract an object's data from Arc, several class methods to apply other transformations,
    a class method to validate the transformed ANS, and a class method to load transformed ANS into a target organization.
    - Start by looking at the doit() method at the bottom of the script.

    Results:
    - Video will exist in target organization's production environment.
    - Script will not encode videos. This can be changed by modifying the Migration Center JSON properties.
    - Script will circulate the video to one replacement website + section in target org,
    or circulate to the target website but the exact same named section, based on parameters passed to script.
    See ( --to-website-section parameter at bottom of script )
    - Multiple website or multiple section logic is not in the scope of this script
    - Video promo images will be imported into the target org.
    - Distributor property in the Video ANS will be written to use the target org distributor ids if they have been created in the target org.
    - Script will attempt to create Distributors used in ANS in the target org.
    - Geographic Restrictions in the Video ANS will be written to use the target org restriction ids if they have been created in the target org.
    - Script will attempt to create Geographic Restrictions used in ANS in the target org.
    - Script will not create Video redirects in the target org.
    There's no way to get a list of redirects attached to a video, without already knowing the specific redirect url.
    Instead video redirects will have to be recreated using a script where the capi is queried specifically for redirect objects.
    See 11_transform_redirects_all.py
    - Does not cause the objects in the references to be ingested to the target organization, other than distributors and geographic restrictions.
    - The returned catalog of references self.references can be used to inform additional operations
    that might be necessary to bring the referenced objects into the target organization.

    Example terminal usage:
    python this_script.py --from-org devtraining --to-org cetest --video-arc_id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token  --to-token cetest prod token --to-website-site cetest --to-website-section /test  --dry-run 1

    :modifies:
        self.references: {}
        self.ans: {}
        self.message: ""
    """
    def __init__(
        self,
        arc_id,
        from_org,
        to_org,
        source_auth,
        target_auth,
        target_website,
        target_section,
        dry_run,
    ):
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.video_arc_id = arc_id
        self.dry_run = bool(int(dry_run))
        self.target_website = target_website
        self.target_section = target_section
        self.ans = {}
        self.message = ""
        self.validation = None
        self.references = DocumentReferences()
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

        # check if this video ans id exists in the target organization
        video_exists_res = requests.get(
            arc_endpoints.get_video_url(self.to_org, "prod"),
            headers=self.arc_auth_header_target,
            params={"uuid": self.video_arc_id},
        )
        if not self.dry_run and video_exists_res.ok and video_exists_res.json():
            self.message = f"video {self.video_arc_id} already exists at {self.to_org}, do not migrate {video_exists_res}"
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
                    f"{video_res} {self.from_org} {self.video_arc_id} {video_res.text}"
                )

    def transform_ans(self):
        """
        removes properties necessary to allow object to be ingested into new org
        sets properties with values appropriate to target org
        sets version to specific ANS version.  Only 0.8.0 ANS version is correct for videos
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
        ] = f"moved orgs from {self.from_org} to {self.to_org}"

    def transform_circulation(self):
        """
        Videos don't retain reference syntax when fetched from the API, but the reference syntax is necessary to ingest a new gallery object.

        reformat `taxonomy.primary_section`, `taxonomy.section` property to use references
        reformat `websites` property
        if target section is passed into script, will circulate to only that section in target website
        otherwise, will circulate to all the original sections that were in the source circulation, but to only the one website
        does not create sections in the target org, only writes references in the ANS
        multiple website or multiple section logic and behavior programming is not in the scope of this script
        the value then in canonical url and websites.{site-id}.website_url will be the same.

        :modifies:
            self.ans
            self.references
            - self.references.circulation contains information-only data
            - there are no Video circulation objects that needs to be ingested, as with a story
            - circulation information is provided as a way to validate the source website/section data vs transformed target website/section data
        """

        # add original circulation info to the references structure
        source_circulation = jmespath.search(
            "[*].{section: _id, website: _website}[]",
            self.ans["taxonomy"]["sections"],
            jmespath.Options(dict_cls=dict),
        )
        self.references.circulation = {self.from_org: source_circulation}
        self.ans["taxonomy"].pop("primary_site", None)
        self.ans["taxonomy"].pop("sites", None)
        self.ans["canonical_website"] = self.target_website

        # WARNING this logic does not capture if the video is published to multiple websites, and each website has a
        # different website_url,
        self.ans.pop("websites", None)
        self.ans["websites"] = {
            self.target_website: {"website_url": self.ans["canonical_url"]}
        }

        # reformat taxonomy.primary_section, sections to use references
        orig_primary_section_id = self.ans["taxonomy"]["primary_section"]["_id"]
        if self.target_section:
            section_reference = {
                "type": "reference",
                "referent": {
                    "id": self.target_section,
                    "type": "section",
                    "website": self.target_website,
                    "referent_properties": {"additional_properties": {"primary": True}},
                },
            }
            self.ans["taxonomy"]["primary_section"] = section_reference
            self.ans["taxonomy"]["sections"] = [section_reference]

        else:
            section_reference = {
                "type": "reference",
                "referent": {
                    "id": orig_primary_section_id,
                    "type": "section",
                    "website": self.target_website,
                    "referent_properties": {"additional_properties": {"primary": True}},
                },
            }
            self.ans["taxonomy"]["primary_section"] = section_reference
            for index, s in enumerate(self.ans["taxonomy"]["sections"]):
                section_id = s["_id"]
                section_reference = {
                    "type": "reference",
                    "referent": {
                        "id": section_id,
                        "type": "section",
                        "website": self.target_website,
                    },
                }
                self.ans["taxonomy"]["sections"][index] = section_reference

        # add updated circulation to the references structure
        target_circulation = jmespath.search(
            "[*].{section: referent.id, website: referent.website}[]",
            self.ans["taxonomy"]["sections"],
            jmespath.Options(dict_cls=dict),
        )
        self.references.circulation = {self.to_org: target_circulation}

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

        # remove the original promo item's anglerfish information from the ans
        try:
            anglerfish_id = self.ans["additional_properties"]["anglerfisharc_id"]
            self.ans["additional_properties"].pop(anglerfish_id)
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

    def transform_distributor(self):
        """
        Figure out what the new distributor id for target org should be, update in ANS
        If distributor does not already exist in target org, script will attempt to create distributor and its restrictions
        If create of target distributor does not work, the distributor.reference_id in story ans will be set to None
        and story ANS will fail validation
        If ANS fails validation because of a None distributor, create the target distributor first with same details as source,
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
                self.target_website,
            )
            self.references.distributor = references_distributor
            self.references.distributor.update(
                {self.from_org: self.to_org}
            ) if references_distributor else None

        if jmespath.search("distributor.reference_id", self.ans):
            orig_dist_id = self.ans["distributor"]["reference_id"]
            if self.dry_run:
                self.ans["distributor"]["reference_id"] = self.dry_run_restriction_msg
                self.references.distributor = {
                    self.from_org: self.to_org,
                    orig_dist_id: self.dry_run_restriction_msg,
                }
            else:
                self.ans["distributor"]["reference_id"] = references_distributor.get(
                    orig_dist_id, None
                )
            # when there's a video distributor, there also can be an illegal entry in credits.affiliation that will break ANS validation
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
        script does not add Authors
            - normally you would write an author as a reference when creating a video if it is an Arc Author
            - author fields in video are returned from the API as local to the document, not as Arc Author object references
            - determining if they are genuine Arc authors would require more API calls and then rewriting the author as a reference if so
            - the payoff of this extra work is not worth the cost since you can get by with sending in the author
            in the same form as it comes back from the API
        script does not create redirects
            - redirects attached to the Video are possible, but they are not represented in the Video ANS directly
            - it is not possible to discover the video redirects using a video's arc id or video canonical url
            - to find video redirects you must query content api `type: redirect` and then run a 2nd query using the
                url returned from 1st query to determine if it is for a video
            - see 11_transform_redirects-all.py

        :modifies:
            self.references
        """
        # related_content property, but remove from ANS if malformed because will fail the ANS validation
        if self.ans.get("related_content", {}).get("basic"):
            self.references.related_content = jmespath.search(
                "related_content.basic[*].{id: _id, type: referent.type}",
                self.ans,
                jmespath.Options(dict_cls=dict),
            )
            if not self.references.related_content:
                self.ans["related_content"]["basic"] = []

    def validate_transform(self):
        # Validate transformed ANS
        video_res2 = requests.post(
            arc_endpoints.ans_validation_url(self.from_org, "0.8.0"),
            headers=self.arc_auth_header_source,
            json=self.ans,
        )
        if video_res2.ok:
            self.validation = True
        else:
            self.message = f"{video_res2} {video_res2.text} "
            self.validation = False
        print("video validation", self.validation, self.video_arc_id)

    def post_transformed_ans(self):
        if not self.dry_run:
            # post transformed ans to new organization
            mc = MigrationJson(
                self.ans, {"video": {"transcoding": False, "useLastUpdated": True}}
            )
            video_res3 = requests.post(
                arc_endpoints.mc_create_ans_url(self.to_org),
                headers=self.arc_auth_header_target,
                json=mc.__dict__,
                params={"ansId": self.video_arc_id, "ansType": "video"},
            )
            print("ans posted to new org's MC", video_res3)

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
        "--to-website-section",
        dest="to_section",
        required=False,
        default="",
        help="target organization's website section id value. If none, original source sections are retained in target object.'",
    )
    parser.add_argument(
        "--video-arc-id",
        dest="video_arc_id",
        required=True,
        default="",
        help="arc id value of video to migrate into target org",
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

    result = Arc2ArcVideo(
        arc_id=args.video_arc_id,
        from_org=args.org,
        to_org=args.to_org,
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        target_website=args.to_website,
        target_section=args.to_section,
        dry_run=args.dry_run,
    ).doit()
    pprint.pp(result)
