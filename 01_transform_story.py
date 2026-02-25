import argparse
import pprint
from dataclasses import dataclass
from typing import Optional

import arc_endpoints
import arc_id
import arc2arc_exceptions
import arc_constants
import dist_ref_id
import jmespath
import json
import requests


@dataclass
class MigrationJson:
    """Data class for Migration Center JSON payload.
    
    Attributes:
        ANS: Transformed ANS dictionary
        circulations: List of circulation objects
        arcAdditionalProperties: Additional properties for Migration Center (e.g., publish settings)
    """
    ANS: dict
    circulations: list
    arcAdditionalProperties: dict


@dataclass
class DocumentReferences:
    """Data class for cataloging references found in transformed content.
    
    Attributes:
        images: Dictionary mapping source image IDs to target image IDs
        galleries: Dictionary mapping source gallery IDs to target gallery IDs
        videos: List of video IDs referenced in the content
        authors: List of author IDs referenced in the content
        distributor: Dictionary mapping source distributor IDs to target distributor IDs
        related_stories: List of related story IDs
        redirects: List of redirect URLs associated with the content
        circulation: Dictionary containing source and target circulation information
    """
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
    python this_script.py --from-org devtraining --to-org cetest --story-arc-id MBDJUMH35VA4VKRW2Y6S2IR44A --from-token devtraining prod token --to-token cetest prod token --to-website-site cetest --to-website-section /test  --dry-run 1

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
            timeout=30
        )
        if story_res.ok:
            story_revision = jmespath.search("draft_revision_id", story_res.json())
            story_res2 = requests.get(
                arc_endpoints.draft_get_story_url(
                    self.from_org, self.story_arc_id, story_revision
                ),
                headers=self.arc_auth_header_source,
                timeout=30
            )
            if story_res2.ok:
                self.ans = jmespath.search("ans", story_res2.json())
                story_res3 = requests.get(
                    arc_endpoints.draft_get_circulations_url(
                        self.from_org, self.story_arc_id
                    ),
                    headers=self.arc_auth_header_source,
                    timeout=30
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
        self.ans["version"] = arc_constants.ANS_VERSION_STANDARD
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
        Re-ids references from Photo Center objects and includes both old and new ids in the return object self.references.
        New ids are used in the rewritten references.

        NOTE: promo_items can support other keys than `.basic` and `.lead_art` but this is not common.
        If your ANS supports more customized promo_items children, customize the logic to accommodate.

        :modifies:
            self.references
            self.ans
        """
        # Step 1: Find all image and gallery references
        image_ids = self._find_image_references()
        gallery_ids = self._find_gallery_references()
        
        # Step 2: Generate new IDs for images and galleries
        image_id_map = self._generate_new_ids(image_ids, self.to_org)
        gallery_id_map = self._generate_new_ids(gallery_ids, self.to_org)
        
        # Step 3: Process nested images within galleries
        nested_image_ids = self._get_nested_gallery_images(gallery_ids)
        nested_image_id_map = self._generate_new_ids(nested_image_ids, self.to_org)
        image_id_map.update(nested_image_id_map)
        
        # Step 4: Update references tracking
        self._update_references_tracking(image_id_map, gallery_id_map)
        
        # Step 5: Replace IDs throughout ANS structure
        combined_id_map = {**image_id_map, **gallery_id_map}
        self._replace_ids_in_ans(combined_id_map)

    def _find_image_references(self):
        """Find all image IDs referenced in content_elements, promo_items, and related_content.
        
        Returns:
            Set of unique image IDs found in the ANS
        """
        image_ids = set()
        
        # Content elements
        ce_images = jmespath.search(
            "content_elements[?referent && referent.type == `image`] | [*].referent.id",
            self.ans
        ) or []
        image_ids.update(ce_images)
        
        # Promo items basic
        pi_images = jmespath.search(
            "(promo_items.basic.*)[?type==`image`].id",
            self.ans
        ) or []
        image_ids.update(pi_images)
        
        # Related content
        rc_images = jmespath.search(
            "related_content.basic[?referent && referent.type == `image`] | [*].referent.id",
            self.ans
        ) or []
        image_ids.update(rc_images)
        
        return image_ids

    def _find_gallery_references(self):
        """Find all gallery IDs referenced in content_elements, promo_items, and related_content.
        
        Returns:
            Set of unique gallery IDs found in the ANS
        """
        gallery_ids = set()
        
        # Content elements
        ce_galleries = jmespath.search(
            "content_elements[?referent && referent.type == `gallery`]._id",
            self.ans
        ) or []
        gallery_ids.update(ce_galleries)
        
        # Promo items lead_art
        pi_galleries = jmespath.search(
            "(promo_items.lead_art.*)[?type==`gallery`].id",
            self.ans
        ) or []
        gallery_ids.update(pi_galleries)
        
        # Related content
        rc_galleries = jmespath.search(
            "related_content.basic[?referent && referent.type == `gallery`]._id",
            self.ans
        ) or []
        gallery_ids.update(rc_galleries)
        
        return gallery_ids

    def _generate_new_ids(self, old_ids, target_org):
        """Generate new Arc IDs for a set of old IDs.
        
        Args:
            old_ids: Set or list of old Arc IDs
            target_org: Target organization identifier
            
        Returns:
            Dictionary mapping old_id -> new_id
        """
        if not old_ids:
            return {}
        
        return {
            old_id: arc_id.generate_arc_id(old_id, target_org)
            for old_id in old_ids
        }

    def _get_nested_gallery_images(self, gallery_ids):
        """Fetch gallery content and extract nested image IDs.
        
        Args:
            gallery_ids: Set of gallery IDs to process
            
        Returns:
            Set of image IDs found within the galleries
        """
        nested_image_ids = set()
        
        for gallery_id in gallery_ids:
            try:
                gallery_res = requests.get(
                    arc_endpoints.get_galleries_url(self.to_org, gallery_id),
                    headers=self.arc_auth_header_source,
                    timeout=30
                )
                
                if gallery_res.ok:
                    content_elements = gallery_res.json().get("content_elements", [])
                    for element in content_elements:
                        if element.get("_id"):
                            nested_image_ids.add(element["_id"])
            except (requests.exceptions.RequestException, KeyError, ValueError, TypeError) as e:
                # Log error but continue processing other galleries
                print(f"Warning: Could not fetch gallery {gallery_id}: {type(e).__name__}")
        
        return nested_image_ids

    def _update_references_tracking(self, image_id_map, gallery_id_map):
        """Update self.references with the ID mappings.
        
        Args:
            image_id_map: Dictionary mapping old image IDs to new IDs
            gallery_id_map: Dictionary mapping old gallery IDs to new IDs
        """
        header = {self.from_org: self.to_org}
        
        if image_id_map:
            self.references.images = {**image_id_map, **header}
        
        if gallery_id_map:
            self.references.galleries = {**gallery_id_map, **header}

    def _replace_ids_in_ans(self, id_map):
        """Replace old IDs with new IDs throughout the ANS structure.
        
        Args:
            id_map: Dictionary mapping old_id -> new_id
        """
        for old_id, new_id in id_map.items():
            # Replace in content_elements
            self._replace_id_in_content_elements(old_id, new_id)
            
            # Replace in related_content
            self._replace_id_in_related_content(old_id, new_id)
            
            # Replace in promo_items.basic
            self._replace_id_in_promo_items_basic(old_id, new_id)
            
            # Replace in promo_items.lead_art
            self._replace_id_in_promo_items_lead_art(old_id, new_id)

    def _replace_id_in_content_elements(self, old_id, new_id):
        """Replace ID in content_elements section of ANS.
        
        Args:
            old_id: Old Arc ID to replace
            new_id: New Arc ID to use
        """
        for element in jmespath.search(f"content_elements[?_id==`{old_id}`]", self.ans) or []:
            element["_id"] = new_id
            if element.get("referent"):
                element["referent"]["id"] = new_id

    def _replace_id_in_related_content(self, old_id, new_id):
        """Replace ID in related_content section of ANS.
        
        Args:
            old_id: Old Arc ID to replace
            new_id: New Arc ID to use
        """
        related_content = self.ans.get("related_content", {}).get("basic")
        if not related_content:
            return
        
        for item in jmespath.search(f"related_content.basic[?_id==`{old_id}`]", self.ans) or []:
            item["_id"] = new_id
            if item.get("referent"):
                item["referent"]["id"] = new_id

    def _replace_id_in_promo_items_basic(self, old_id, new_id):
        """Replace ID in promo_items.basic section of ANS.
        
        Args:
            old_id: Old Arc ID to replace
            new_id: New Arc ID to use
        """
        promo_basic = self.ans.get("promo_items", {}).get("basic")
        if not promo_basic:
            return
        
        if jmespath.search(f"promo_items.basic._id==`{old_id}`", self.ans):
            promo_basic["_id"] = new_id
            if promo_basic.get("referent"):
                promo_basic["referent"]["id"] = new_id

    def _replace_id_in_promo_items_lead_art(self, old_id, new_id):
        """Replace ID in promo_items.lead_art section of ANS.
        
        Args:
            old_id: Old Arc ID to replace
            new_id: New Arc ID to use
        """
        promo_lead_art = self.ans.get("promo_items", {}).get("lead_art")
        if not promo_lead_art:
            return
        
        if jmespath.search(f"promo_items.lead_art._id==`{old_id}`", self.ans):
            promo_lead_art["_id"] = new_id
            if promo_lead_art.get("referent"):
                promo_lead_art["referent"]["id"] = new_id

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
        self._extract_story_references()
        self._extract_author_references()
        self._cleanup_credits_version()
        self._extract_video_references()

    def _extract_story_references(self):
        """Extract related story IDs from related_content.basic and set self.references.related_stories.
        
        Story IDs do not need to be regenerated when moving between orgs.
        """
        self.references.related_stories = (
            jmespath.search(
                "related_content.basic[?referent && referent.type == `story`]._id",
                self.ans,
            )
            or []
        )

    def _extract_author_references(self):
        """Extract author IDs from credits.by and set self.references.authors.
        
        Author IDs do not need to be regenerated when moving between orgs.
        """
        self.references.authors = jmespath.search("credits.by[*].referent.id", self.ans) or []

    def _cleanup_credits_version(self):
        """Remove .version from credits.by entries so ANS passes validation.
        
        credits.by in guest/local format won't pass validation if .version property is included,
        and is also mismatched with current top-level .version property.
        """
        authors = jmespath.search("credits.by[*].name", self.ans)
        if authors:
            for index, c in enumerate(self.ans["credits"]["by"]):
                try:
                    self.ans["credits"]["by"][index].pop("version", None)
                except (KeyError, IndexError, TypeError):
                    pass

    def _extract_video_references(self):
        """Extract video IDs from content_elements, promo_items, and related_content; set self.references.videos.
        
        Video IDs do not need to be regenerated when moving between orgs.
        """
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
        """Validate the transformed ANS against Arc's validation endpoint.
        
        Sets self.validation to True if validation passes, False otherwise.
        Raises MakingNewDistributorFirstTimeException if validation fails due to
        creating a new distributor (expected on first attempt).
        
        :modifies:
            self.validation
            self.message
        """
        # Validate transformed ANS
        try:
            story_res4 = requests.post(
                arc_endpoints.ans_validation_url(self.to_org),
                headers=self.arc_auth_header_target,
                json=self.ans,
                timeout=30
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
        """Post transformed ANS to target organization via Migration Center.
        
        Creates a MigrationJson payload and posts it to the Migration Center endpoint.
        Sets self.message if posting fails.
        
        :modifies:
            self.message
        """
        # post transformed ans to new organization
        mc = MigrationJson(self.ans, self.circulation, {"story": {"publish": True}})
        self.message = None
        try:
            story_res5 = requests.post(
                arc_endpoints.mc_create_ans_url(self.to_org),
                headers=self.arc_auth_header_target,
                json=mc.__dict__,
                params={"ansId": self.story_arc_id, "ansType": "story"},
                timeout=30
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
            timeout=30
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
                            timeout=30
                        )
                        if not story_res7.ok:
                            raise arc2arc_exceptions.ArcRedirectAlreadyExistsFailed
                    except arc2arc_exceptions.ArcRedirectAlreadyExistsFailed:
                        raise
                    except (requests.exceptions.RequestException, KeyError, ValueError, TypeError) as e:
                        error_msg = ""
                        try:
                            error_msg = story_res7.json().get("error_message", str(e))
                        except (AttributeError, ValueError, json.JSONDecodeError):
                            error_msg = str(e)
                        print(f"redirect error: {error_msg}")
                    else:
                        print("redirect created", story_res7.json())

    def doit(self):
        """Execute the complete transformation workflow.
        
        Orchestrates the ETL process: fetch, transform, validate, and post.
        Returns early if fetch fails or validation fails.
        
        Returns:
            Dictionary containing references, ans, and circulation if successful.
            Tuple of (message, None) if an error occurs.
        """
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
