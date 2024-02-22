import argparse
import pprint
from dataclasses import dataclass
from typing import Optional

import arc_endpoints
import arc_id
import arc2arc_exceptions
import dist_ref_id
import jmespath
import json
import requests


@dataclass
class MigrationJson:
    ANS: dict
    circulations: list
    arcAdditionalProperties: dict


@dataclass
class DocumentReferences:
    images: Optional[dict] = None
    galleries: Optional[dict] = None
    videos: Optional[list] = None
    authors: Optional[list] = None
    distributor: Optional[dict] = None
    related_stories: Optional[list] = None
    redirects: Optional[list] = None
    circulation: Optional[dict] = None


class Arc2ArcStory:
    """
    Usage: Copy one Story via its arc id from source organization (prod) into target organization (prod).
    The script models the simplest transformation of Story and Story Circulation.  THe script is not meant to cover all
    complex circumstances. The script is not for production use, it is to demonstrate the transformations needed to
    change ANS from one Arc organization to be loaded into another Arc organization.
    - The script sets up a class where an ETL process takes place
    - CLass properties are modified by class methods, resulting in the transformed ANS.
    - There is a class method to extract an object's data from Arc, several class methods to apply other transformations,
    a class method to validate the transformed ANS, and a class method to load transformed ANS into a target organization.
    - Start by looking at the doit() method at the bottom of the script.

    Results:
    - Story will exist in target organization's production environment.
    - References to Arc objects used in Story will be cataloged and a list of them are returned.
    - The objects behind the references will not be moved into the target organization unless they are Distributors.
    - Story circulation will be created in target organization based on parameters passed into the script.
    - Image and Gallery references are reconstructed to use new image ids.
    This re-id is necessary when moving Photo Center objects to a new org.
    It is not possible to maintain the same image and gallery ids between different orgs.
    - Video IDs in references are not regenerated, as the same video ids can be used between the old and new org.
    - Distributor property in the ANS will be written to use the target org distributor ids if they have been created in the target org.
    - Script will attempt to create Distributors used in ANS in the target org.
    - The script will create document redirects for this story in the source org to the target org.
    - When the script is complete it will display an object showing the document references and redirects that were associated
    with the source object, self.references
    - Does not cause the objects in the references to be ingested to the target organization, other than distributors.
    - The returned catalog of references self.references can be used to inform additional operations
    that might be necessary to bring the referenced objects into the target organization.

    Example terminal usage:
    python this_script.py --from-org devtraining --to-org cetest --story-arc_id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token --to-token cetest prod token --to-website-site cetest --to-website-section /test  --dry-run 1

    :modifies:
        self.references: {}
        self.ans: {}
        self.circulation: {}
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
        self.dry_run = bool(int(dry_run))
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.from_website = ""
        self.story_arc_id = arc_id
        self.target_website = target_website
        self.target_section = target_section
        self.ans = {}
        self.circulation = []
        self.references = DocumentReferences()
        self.validation = None
        self.message = ""
        self.dry_run_restriction_msg = "new distributors not created during a dry run"

    def fetch_source_ans(self):
        """ Extract ANS from source organization
        :modifies:
            self.ans
            self.circulation
            self.message
        """
        if self.dry_run:
            self.references.redirects = ["Story Redirects will not be evaluated during a dry run"]
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
                    self.message = f"{story_res3} {story_res3.text}"
            else:
                self.message = f"{story_res2} {story_res2.text}"
        else:
            self.message = f"{story_res} {story_res.text}"

    def transform_ans(self):
        """
        removes ANS properties necessary to allow ANS to be ingested into new org
        sets properties with values appropriate to target org
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
        ] = f"moved orgs from {self.from_org} to {self.to_org}"
        self.ans["canonical_website"] = self.target_website

    def transform_distributor(self):
        """
        Figure out what the new distributor id for target org should be, update in ANS
        Uses a helper script to do the complex work of finding ir creating existing distributor id in target org
        If no distributor already exists in target org, script will attempt to create distributor and its restrictions
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
                self.target_website,
            )
            self.references.distributor = references_distributor
            self.references.distributor.update({self.from_org: self.to_org}) if references_distributor else None

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

    def transform_photo_references(self):
        """
        Finds and rewrites Photo Center references in ANS that will need to be ingested into target organization.
        Re-ids references from Photo Center objects and includes both old and new ids in the return object self.
        references
        New ids are used in the rewritten references.

        NOTE: promo_items can support other keys than `.basic` and `.lead_art` but this is not common.
        If your ANS supports more customized promo_items children, customize the logic to accommodate.

        :modifies:
            self.references
            self.ans
        """
        references_images_newids = {}
        references_galleries_newids = {}
        combined_newids = {}
        header = {self.from_org: self.to_org}

        # Filter content_elements, promo_items.basic, related_content for
        # `referent.type: images` and select out the `$._id` or `$.referent.id` to add to self.references for return display
        ce_imgs = (
            jmespath.search(
                "content_elements[?referent && referent.type == `image`] | [*].referent.id",
                self.ans,
            )
            or []
        )
        pi_imgs = jmespath.search("(promo_items.basic.*)[?type==`image`].id", self.ans) or []
        rc_img = (
            jmespath.search(
                "related_content.basic[?referent && referent.type == `image`] | [*].referent.id",
                self.ans,
            )
            or []
        )
        references_images = list(set(ce_imgs + pi_imgs + rc_img)) or None

        # Filter content_elements, promo_items.lead_art ,related_content for
        # `referent.type: gallery` and select out the `$._id` or `$.referent.id` to add to self.references for return display
        ce_gals = (
            jmespath.search(
                "content_elements[?referent && referent.type == `gallery`]._id",
                self.ans,
            )
            or []
        )
        pi_gals = (
            jmespath.search("(promo_items.lead_art.*)[?type==`gallery`].id", self.ans) or []
        )
        rc_gals = (
            jmespath.search(
                "related_content.basic[?referent && referent.type == `gallery`]._id",
                self.ans,
            )
            or []
        )
        references_galleries = list(set(ce_gals + pi_gals + rc_gals)) or None

        # Generate new arc ids for Photo Center objects that are moving to a new organization.
        #   There are some organizations that are rare exceptions to this restriction/
        # NOTE: Story and Video objects do not need new arc ids when moving to a new organization.
        if references_images:
            for img in references_images:
                references_images_newids.update(
                    {img: arc_id.generate_arc_id(img, self.to_org)}
                )
            self.references.images = references_images_newids
            self.references.images.update(header)

        if references_galleries:
            for gal in references_galleries:
                references_galleries_newids.update(
                    {gal: arc_id.generate_arc_id(gal, self.to_org)}
                )
            self.references.galleries = references_galleries_newids
            self.references.galleries.update(header)

            # If there are galleries, get the image ids inside and add these images to the image references list
            for gal_id in references_galleries:
                gallery_res = requests.get(
                    arc_endpoints.get_galleries_url(self.to_org, gal_id),
                    headers=self.arc_auth_header_source,
                )
                if gallery_res.ok:
                    # make the image ids in the gallery content_elements unique. hash(original_id + org_id)
                    gal_imgs = gallery_res.json()["content_elements"]
                    for index, element in enumerate(gal_imgs):
                        old_id = element["_id"]
                        regen_id = arc_id.generate_arc_id(old_id, self.to_org)
                        references_images_newids.update({old_id: regen_id})
                        self.references.images.update(references_images_newids)

        # replace Photo Center arc ids in the ANS references with regenerated values: in content elements, promo items, related contents
        combined_newids.update(references_images_newids)
        combined_newids.update(references_galleries_newids)
        for newid in combined_newids:
            # content elements
            for this_ce in jmespath.search(f"content_elements[?_id==`{newid}`]", self.ans):
                this_ce["_id"] = combined_newids[newid]
                this_ce["referent"]["id"] = combined_newids[newid]
            # related_content
            if self.ans.get("related_content", {}).get("basic"):
                for this_rc in jmespath.search(
                    f"related_content.basic[?_id==`{newid}`]", self.ans
                ):
                    this_rc["_id"] = combined_newids[newid]
                    this_rc["referent"]["id"] = combined_newids[newid]
            # featured media image is in promo_items.basic
            if jmespath.search(f"promo_items.basic._id==`{newid}`", self.ans):
                self.ans["promo_items"]["basic"]["_id"] = combined_newids[newid]
                self.ans["promo_items"]["basic"]["referent"]["id"] = combined_newids[
                    newid
                ]
            # featured media gallery is in promo_items.lead_art
            if jmespath.search(f"promo_items.lead_art._id==`{newid}`", self.ans):
                self.ans["promo_items"]["lead_art"]["_id"] = combined_newids[newid]
                self.ans["promo_items"]["lead_art"]["referent"]["id"] = combined_newids[
                    newid
                ]

    def transform_circulation(self):
        """
        Rewrites the circulation object for ingestion into the target organization.
        :modifies:
            self.circulation
            self.references
            self.from_website
        """
        # filter for original circulation section and website values
        source_circulation = jmespath.search(
            "[*].website_sections[*].{section: referent.id, website: referent.website}[]",
            self.circulation,
            jmespath.Options(dict_cls=dict),
        )
        # set source circulation information for return display to see the changes between source and target circulation after script completes
        self.references.circulation = {self.from_org: source_circulation}
        # self.from_website is being set for use in the method that re-creates the story redirects in the target org
        self.from_website = source_circulation[0]["website"]

        # Either reset the first circulated section to the section value passed in the script args and drop other sections if any,
        # or if script args section value is `none`, leave original sections values as is and only reset the target's website value
        for circ in self.circulation:
            circ["website_id"] = circ["website_primary_section"]["referent"]["website"] = self.target_website
            for circ2 in circ["website_sections"]:
                circ2["referent"]["website"] = self.target_website
                if self.target_section:
                    circ2["referent"]["id"] = self.target_section
            if self.target_section:
                circ["website_primary_section"]["referent"]["id"] = self.target_section
                break

        # filter the target circulation for only the section and website data; add to self.references for return display
        target_circulation = jmespath.search(
            "[*].website_sections[*].{section: referent.id, website: referent.website}[]",
            self.circulation,
            jmespath.Options(dict_cls=dict),
        )
        # set information for return display to make clear what changes occurred between source and target circulation
        self.references.circulation.update({self.to_org: target_circulation})

    def other_supporting_references(self):
        """
        Finds references in ANS that will need to be ingested into target organization.
        Sets information for return display in self.references.
        Does some reformatting of ANS references as necessary to ensure Validation of the ANS prior to ingestion.

        :modifies:
            self.references
            self.ans
        """
        # Are there supporting story references in the ans? Build list of story references and add to self.references return display.
        # Story ids do not need to be regenerated.
        self.references.related_stories = (
            jmespath.search(
                "related_content.basic[?referent && referent.type == `story`]._id",
                self.ans,
            )
            or []
        )

        # Are there author references in the ans? Build list of author references and add to self.references for return display.
        # Author ids do not need to be regenerated.
        self.references.authors = jmespath.search("credits.by[*].referent.id", self.ans) or []

        # credits.by in guest/local format won't pass validation if `.version` property is included, and is
        #   also mismatched with current top-level `.version` property. Leave credits.by author information, remove  `.version` property.
        authors = jmespath.search("credits.by[*].name", self.ans)
        if authors:
            for index, c in enumerate(self.ans["credits"]["by"]):
                try:
                    self.ans["credits"]["by"][index].pop("version", None)
                except Exception:
                    pass

        # Build list of video references and add to return display. Video ids do not need to be regenerated.
        # Filter content_elements, promo_items.lead_art, promo_items.basic, related_content for
        # `referent.type: video` and select out the `referent._id` to add to the self.references for return display
        ce_vids = (
            jmespath.search(
                "content_elements[?referent && referent.type == `video`]._id", self.ans
            )
            or []
        )
        pi_vids = jmespath.search("(promo_items.lead_art.*)[?type==`video`].id", self.ans) or []
        rc_vids = (
            jmespath.search(
                "related_content.basic[?referent && referent.type == `video`]._id",
                self.ans,
            )
            or []
        )
        self.references.videos = list(set(ce_vids + pi_vids + rc_vids)) or None

    def validate_transform(self):
        # Validate transformed ANS
        try:
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

            # raise custom error only if the error is due to creating a new distributor. should only happen the first time a new distributor is attempted.
            if story_res4.status_code == 400 and jmespath.search("[*].message", json.loads(story_res4.text)) == ['should NOT have additional properties', 'should be equal to one of values', 'should be string', 'should match exactly one schema in oneOf']:
                raise arc2arc_exceptions.MakingNewDistributorFirstTimeException

        except Exception as e:
            self.message = f"{str(e)} full error: {story_res4.text}" if e.__module__ == "arc2arc_exceptions" else f"{story_res4} {story_res4.text}"
        else:
            print("story validation", self.validation, self.story_arc_id)

    def post_transformed_ans(self):
        # post transformed ans to new organization
        mc = MigrationJson(self.ans, self.circulation, {"story": {"publish": True}})
        self.message = None
        try:
            story_res5 = requests.post(
                arc_endpoints.mc_create_ans_url(self.to_org),
                headers=self.arc_auth_header_target,
                json=mc.__dict__,
                params={"ansId": self.story_arc_id, "ansType": "story"},
            )

            if not story_res5.ok:
                raise arc2arc_exceptions.ArcObjectToMigrationCenterFailed

        except Exception as e:
            self.message = f"{str(e)} {story_res5.status_code} {story_res5.reason} {story_res5.text}"
        else:
            print(f"ans posted to {self.to_org} Migration Center", story_res5)

    def document_redirects(self):
        """
        Process document level redirects into the target org
        """
        story_res6 = requests.get(
            arc_endpoints.get_story_redirects_url(
                self.from_org, self.story_arc_id, self.from_website
            ),
            headers=self.arc_auth_header_source,
        )
        if story_res6.ok:
            redirects = story_res6.json()["redirects"]
            # set information for return display in self.references
            self.references.redirects = redirects
            # attempt to create redirects in target organization
            if not self.dry_run:
                for red_url in redirects:
                    try:
                        story_res7 = requests.post(
                            arc_endpoints.get_story_redirects_url(
                                self.to_org,
                                self.story_arc_id,
                                self.target_website,
                                red_url["website_url"],
                            ),
                            headers=self.arc_auth_header_target,
                            json={"document_id": self.story_arc_id},
                        )
                        if not story_res7.ok:
                            raise arc2arc_exceptions.ArcRedirectAlreadyExistsFailed
                    except Exception as e:
                        print("redirect", story_res7.json().get("error_message", ""), str(e))
                    else:
                        print("redirect created", story_res7.json())

    def doit(self):
        self.fetch_source_ans()
        if not self.ans and not self.circulation:
            return self.message, None
        self.transform_ans()
        self.transform_distributor()
        self.transform_photo_references()
        self.other_supporting_references()
        self.transform_circulation()
        self.validate_transform()
        if not self.validation:
            return self.message, None
        elif not self.dry_run:
            self.post_transformed_ans()
            if not self.message:
                self.document_redirects()
            else:
                print(self.message)
        return {"references": self.references.__dict__, "ans": self.ans, "circulation": self.circulation}


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
        "--to-website-site",
        dest="to_website",
        help="target organization's website name",
        required=True,
        default="",
    )
    parser.add_argument(
        "--to-website-section",
        dest="to_section",
        help="target organization's website section id value.  If none, source sections are retained.",
        required=False,
        default="",
    )
    parser.add_argument(
        "--story-arc-id",
        dest="story_arc_id",
        help="arc id value of story to migrate into target org",
        required=True,
        default="",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        help="Set this to 1 to test the results of transforming an object. The object will not actually post to the target org.",
        required=False,
        default=0,
    )
    args = parser.parse_args()

    arc_auth_header_source = {"Authorization": f"Bearer {args.from_token}"}
    arc_auth_header_target = {"Authorization": f"Bearer {args.to_token}"}

    result = Arc2ArcStory(
        arc_id=args.story_arc_id,
        from_org=args.org,
        to_org=args.to_org,
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        target_website=args.to_website,
        target_section=args.to_section,
        dry_run=args.dry_run,
    ).doit()

    print('\nRESULTS')
    pprint.pp(result)
