"""Profile for Edna, running Ubuntu 20.04

Instructions:
See https://github.com/tslilyai/edna/blob/main/README.md
"""

# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as pg

# Create a portal context.
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()


pc.defineParameter("DATASET", "URN of your image-backed dataset", 
                   portal.ParameterType.STRING,
                   "urn:publicid:IDN+utah.cloudlab.us:edna-pg0+imdataset+DataWithRepo")
pc.defineParameter("MPOINT", "Mountpoint for file system",
                   portal.ParameterType.STRING, "/data")

params = pc.bindParameters()

node = request.RawPC("mynode")
node.disk_image = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU20-64-STD'

# Request a specific number of VCPUs.
node.cores = 16

# Request a specific amount of memory (in GB).
node.ram = 60

# add the blockstore
bs = node.Blockstore("bs", params.MPOINT)
bs.dataset = params.DATASET

#bs.size = "100GB"

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
